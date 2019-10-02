/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.ReadOnlyGridCacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.preload.GridPartitionBatchDemandMessage;
import org.apache.ignite.internal.processors.cache.preload.PartitionUploadManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.GridTopic.TOPIC_REBALANCE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;

/** */
public class GridCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    public static final String REBALANCE_CP_REASON = "Rebalance has been scheduled [grps=%s]";

    /** */
    private static final Runnable NO_OP = () -> {};

    /** */
    public static final int REBALANCE_TOPIC_IDX = 0;

    /** todo */
    private static final boolean presistenceRebalanceEnabled = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, false);

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Checkpoint listener. */
    private final CheckpointListener cpLsnr = new CheckpointListener();

    /** */
    private final ConcurrentMap<UUID, RebalanceDownloadFuture> futMap = new ConcurrentHashMap<>();

    /** */
    private volatile RebalanceDownloadFuture headFut = new RebalanceDownloadFuture();

    /** */
    private PartitionUploadManager uploadMgr;

    /**
     * @param ktx Kernal context.
     */
    public GridCachePreloadSharedManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) :
            "Persistence must be enabled to preload any of cache partition files";

        uploadMgr = new PartitionUploadManager(ktx);
    }

    /**
     * @return The Rebalance topic to communicate with.
     */
    public static Object rebalanceThreadTopic() {
        return TOPIC_REBALANCE.topic("Rebalance", REBALANCE_TOPIC_IDX);
    }

    /**
     * @param fut The future to check.
     * @return <tt>true</tt> if future can be processed.
     */
    private boolean staleFuture(GridFutureAdapter<?> fut) {
        return fut == null || fut.isCancelled() || fut.isFailed() || fut.isDone();
    }

    public boolean persistenceRebalanceApplicable() {
        return !cctx.kernalContext().clientNode() &&
            CU.isPersistenceEnabled(cctx.kernalContext().config()) &&
            cctx.isRebalanceEnabled();
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        uploadMgr.start0(cctx);

        ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(cpLsnr);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            uploadMgr.stop0(cancel);

            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(cpLsnr);

            for (RebalanceDownloadFuture rebFut : futMap.values())
                rebFut.cancel();

            futMap.clear();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param assignsMap The map of cache groups assignments to process.
     * @return The map of cache assignments <tt>[group_order, [node, [group_id, partitions]]]</tt>
     */
    private NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> sliceNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap
    ) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> result = new TreeMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> grpEntry : assignsMap.entrySet()) {
            int grpId = grpEntry.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPreloaderAssignments assigns = grpEntry.getValue();

            if (cctx.filePreloader().fileRebalanceRequired(grp, assigns)) {
                int grpOrderNo = grp.config().getRebalanceOrder();

                result.putIfAbsent(grpOrderNo, new HashMap<>());

                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> grpAssigns : assigns.entrySet()) {
                    ClusterNode node = grpAssigns.getKey();

                    result.get(grpOrderNo).putIfAbsent(node, new HashMap<>());

                    result.get(grpOrderNo)
                        .get(node)
                        .putIfAbsent(grpId,
                            grpAssigns.getValue()
                                .partitions()
                                .fullSet());
                }
            }
        }

        return result;
    }

    /**
     * @param assignsMap A map of cache assignments grouped by grpId.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap,
        AffinityTopologyVersion topVer,
        boolean force,
        long rebalanceId
    ) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> nodeOrderAssignsMap =
            sliceNodeCacheAssignments(assignsMap);

        if (nodeOrderAssignsMap.isEmpty())
            return NO_OP;

        // Start new rebalance session.
        final RebalanceDownloadFuture headFut0 = headFut;

        if (!headFut0.isDone())
            headFut0.cancel();

        lock.writeLock().lock();

        try {
            RebalanceDownloadFuture rqFut = null;
            Runnable rq = NO_OP;

            if (log.isInfoEnabled())
                log.info("Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            // Clear the previous rebalance futures if exists.
            futMap.clear();

            for (Map<ClusterNode, Map<Integer, Set<Integer>>> descNodeMap : nodeOrderAssignsMap.descendingMap().values()) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> assignEntry : descNodeMap.entrySet()) {
                    RebalanceDownloadFuture rebFut = new RebalanceDownloadFuture(cctx, log, assignEntry.getKey(),
                        rebalanceId, assignEntry.getValue(), topVer, cpLsnr);

                    final Runnable nextRq0 = rq;
                    final RebalanceDownloadFuture rqFut0 = rqFut;

                    if (rqFut0 == null)
                        headFut = rebFut; // The first seen rebalance node.
                    else {
                        rebFut.listen(f -> {
                            try {
                                if (log.isDebugEnabled())
                                    log.debug("Running next task, last future result is " + f.get());

                                if (f.get()) // Not cancelled.
                                    nextRq0.run();
                                // todo check how this chain is cancelling
                            }
                            catch (IgniteCheckedException e) {
                                rqFut0.onDone(e);
                            }
                        });
                    }

                    rq = requestNodePartitions(assignEntry.getKey(), rebFut);
                    rqFut = rebFut;
                }
            }

            // create listener
            TransmissionHandler hndr = new RebalanceDownloadHandler();

            cctx.kernalContext().io().addTransmissionHandler(rebalanceThreadTopic(), hndr);

            headFut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                @Override public void applyx(IgniteInternalFuture<Boolean> fut0) throws IgniteCheckedException {
                    cctx.kernalContext().io().removeTransmissionHandler(rebalanceThreadTopic());

                    if (fut0.get())
                        U.log(log, "The final persistence rebalance future is done [result=" + fut0.isDone() + ']');
                }
            });

            log.debug("Returing first runnable");

            return rq;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param node Clustre node to send inital demand message to.
     * @param rebFut The future to handle demand request.
     */
    private Runnable requestNodePartitions(
        ClusterNode node,
        RebalanceDownloadFuture rebFut
    ) {
        return new Runnable() {
            @Override public void run() {
                if (staleFuture(rebFut))
                    return;

                if (log.isInfoEnabled())
                    log.info("Start partitions preloading [from=" + node.id() + ", fut=" + rebFut + ']');

                final Map<Integer, Set<Integer>> assigns = rebFut.assigns;

                IgniteInternalFuture<Void> switchFut = cpLsnr.schedule(() -> {
                        for (Map.Entry<Integer, Set<Integer>> e : assigns.entrySet()) {
                            CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());

                            for (Integer partId : e.getValue()) {
                                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                                if (part.readOnly())
                                    continue;

                                part.readOnly(true);
                            }
                        }
                    });

                if (log.isDebugEnabled())
                    log.debug("Await partition switch: " + assigns);

                try {
                    if (!switchFut.isDone())
                        cctx.database().wakeupForCheckpoint(String.format(REBALANCE_CP_REASON, assigns.keySet()));

                    switchFut.get();
                }
                catch (IgniteCheckedException e) {
                    rebFut.onDone(e);

                    // todo throw exception?
                    return;
                }

                for (Map.Entry<Integer, Set<Integer>> e : assigns.entrySet()) {
                    int grpId = e.getKey();

                    CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                    for (Integer partId : e.getValue()) {
                        GridDhtLocalPartition part = gctx.topology().localPartition(partId);

                        if (log.isDebugEnabled())
                            log.debug("Add destroy future for partition " + part.id());

                        evictPartitionAsync(part).listen(fut -> {
                            try {
                                if (!fut.get())
                                    throw new IgniteCheckedException("Partition was not destroyed " +
                                        "properly [grp=" + gctx.cacheOrGroupName() + ", p=" + part.id() + "]");

                                boolean exists = gctx.shared().pageStore().exists(grpId, part.id());

                                assert !exists : "File exists [grp=" + gctx.cacheOrGroupName() + ", p=" + part.id() + "]";

                                rebFut.onPartitionEvicted(grpId, partId);
                            }
                            catch (IgniteCheckedException ex) {
                                rebFut.onDone(ex);
                            }
                        });
                    }
                }

                try {
                    if (rebFut.initReq.compareAndSet(false, true)) {
                        if (log.isDebugEnabled())
                            log.debug("Prepare demand batch message [rebalanceId=" + rebFut.rebalanceId + "]");

                        GridPartitionBatchDemandMessage msg0 =
                            new GridPartitionBatchDemandMessage(rebFut.rebalanceId,
                                rebFut.topVer,
                                assigns.entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                        e -> GridIntList.valueOf(e.getValue()))));

                        futMap.put(node.id(), rebFut);

                        cctx.gridIO().sendToCustomTopic(node, rebalanceThreadTopic(), msg0, SYSTEM_POOL);

                        if (log.isDebugEnabled())
                            log.debug("Demand message is sent to partition supplier [node=" + node.id() + "]");
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Error sending request for demanded cache partitions", e);

                    rebFut.onDone(e);

                    futMap.remove(node.id());
                }
            }
        };
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean fileRebalanceRequired(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        return FileRebalanceSupported(grp, assigns) &&
            grp.config().getRebalanceDelay() != -1 &&
            grp.config().getRebalanceMode() != CacheRebalanceMode.NONE;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean FileRebalanceSupported(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        if (assigns == null || assigns.isEmpty())
            return false;

        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        if (grp.mvccEnabled())
            return false;

        return presistenceRebalanceEnabled &&
            grp.persistenceEnabled() &&
            IgniteFeatures.allNodesSupports(assigns.keySet(), IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

//    /**
//     * @param grp The corresponding to assignments cache group context.
//     * @param topVer Topology versions to calculate assignmets at.
//     * @return {@code True} if cache might be rebalanced by sending cache partition files.
//     */
//    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, AffinityTopologyVersion topVer) {
//        AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);
//
//        // All of affinity nodes must support to new persistence rebalance feature.
//        List<ClusterNode> affNodes =  aff.idealAssignment().stream()
//            .flatMap(List::stream)
//            .collect(Collectors.toList());
//
//
//    }

//    /**
//     * @param grp The corresponding to assignments cache group context.
//     * @param nodes The list of nodes to check ability of file transferring.
//     * @return {@code True} if cache might be rebalanced by sending cache partition files.
//     */
//    private boolean rebalanceByPartitionSupported(CacheGroupContext grp, Collection<ClusterNode> nodes) {
//
//    }

//    /**
//     * @return The instantiated upload mamanger.
//     */
//    public PartitionUploadManager upload() {
//        return uploadMgr;
//    }

    /**
     * @param fut Exchange future.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture fut) {
        // todo switch to read-only mode after first exchange
        System.out.println(cctx.localNodeId() + " >xxx> process onExchangeDone");

        // switch partitions without exchange
    }

    /**
     * Completely destroy partition without changing state in node2part map.
     *
     * @param part
     * @return
     */
    private IgniteInternalFuture<Boolean> evictPartitionAsync(GridDhtLocalPartition part) {
        GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

        part.clearAsync();

        part.onClearFinished(c -> {
            //todo should prevent any removes on DESTROYED partition.
            ReadOnlyGridCacheDataStore store = (ReadOnlyGridCacheDataStore)part.dataStore().store(true);

            store.disableRemoves();

            try {
                part.group().offheap().destroyCacheDataStore(part.dataStore()).listen(f -> {
                        try {
                            fut.onDone(f.get());
                        }
                        catch (IgniteCheckedException e) {
                            fut.onDone(e);
                        }
                    }
                );
            }
            catch (IgniteCheckedException e) {
                fut.onDone(e);
            }
        });

        return fut;
    }

    /**
     * Restore partition on new file. Partition should be completely destroyed before restore it with new file.
     *
     * @param grpId Group id.
     * @param partId Partition number.
     * @param fsPartFile New partition file on the same filesystem.
     * @return Future that will be completed when partition will be fully re-initialized. The future result is the HWM
     * value of update counter in read-only partition.
     * @throws IgniteCheckedException If file store for specified partition doesn't exists or partition file cannot be
     * moved.
     */
    public IgniteInternalFuture<T2<Long, Long>> restorePartition(
        int grpId,
        int partId,
        File fsPartFile,
        IgniteInternalFuture destroyFut
    ) throws IgniteCheckedException {
        CacheGroupContext ctx = cctx.cache().cacheGroup(grpId);

        if (!destroyFut.isDone()) {
            if (log.isDebugEnabled())
                log.debug("Await partition destroy [grp=" + grpId + ", partId=" + partId + "]");

            destroyFut.get();
        }

        File dst = new File(storePath(grpId, partId));

        log.info("Moving downloaded partition file: " + fsPartFile + " --> " + dst);

        try {
            Files.move(fsPartFile.toPath(), dst.toPath());
        }
        catch (IOException e) {
            // todo FileAlreadyExistsException -> retry ?
            throw new IgniteCheckedException("Unable to move file from " + fsPartFile + " to " + dst, e);
        }

        // Reinitialize file store afte rmoving partition file.
        cctx.pageStore().ensure(grpId, partId);

        return cpLsnr.schedule(() -> {
            // Save current update counter.
            PartitionUpdateCounter maxCntr = ctx.topology().localPartition(partId).dataStore().partUpdateCounter();

            // Replacing partition and cache data store with the new one.
            // After this operation all on-heap cached entries should be cleaned.
            // At this point all partition updates are queued.
            // File page store should be reinitialized.
            assert cctx.pageStore().exists(grpId, partId) : "File doesn't exist [grpId=" + grpId + ", p=" + partId + "]";

            GridDhtLocalPartition part = ctx.topology().forceCreatePartition(partId, true);

            // Switching to new datastore.
            part.readOnly(false);

            maxCntr.finalizeUpdateCounters();

            return new T2<>(part.updateCounter(), maxCntr.get());
        });
    }

//    // todo protected
//    private IgniteInternalFuture<Void> offerCheckpointTask(final Runnable task) {
//        return offerCheckpointTask(() -> {
//           task.run();
//
//           return null;
//        });
//    }

//    private <R> IgniteInternalFuture<R> offerCheckpointTask(final Callable<R> task) {
//        CheckpointTask<R> task0 = new CheckpointTask<>(task);
//
//        checkpointLsnr.schedule(task0);
//
//        return task0.fut;
//    }

    public void handleDemandMessage(UUID nodeId, GridPartitionBatchDemandMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Handling demand request " + msg.rebalanceId());

        if (msg.rebalanceId() < 0) // Demand node requested context cleanup.
            return;

        ClusterNode demanderNode = cctx.discovery().node(nodeId);

        if (demanderNode == null) {
            log.error("The demand message rejected (demander node left the cluster) ["
                + ", nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']');

            return;
        }

        if (msg.assignments() == null || msg.assignments().isEmpty()) {
            log.error("The Demand message rejected. Node assignments cannot be empty ["
                + "nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']');

            return;
        }

        uploadMgr.onDemandMessage(nodeId, msg, PUBLIC_POOL);
    }

    /**
     * Get partition restore future.
     *
     * @param msg Message.
     * @return Partition restore future or {@code null} if no partition currently restored.
     */
    public IgniteInternalFuture partitionRestoreFuture(GridCacheMessage msg) {
        if (futMap.isEmpty())
            return null;

        if (!(msg instanceof GridCacheGroupIdMessage) && !(msg instanceof GridCacheIdMessage))
            return null;

        assert futMap.size() == 1 : futMap.size();

        RebalanceDownloadFuture rebFut = futMap.values().iterator().next();

        // todo how to get partition and group
        return rebFut.switchFut(-1, -1);
    }

    /**
     * Get partition file path.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return Absolute partition file path
     * @throws IgniteCheckedException If cache or partition with the given ID was not created.
     */
    private String storePath(int grpId, int partId) throws IgniteCheckedException {
        return ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId)).getFileAbsolutePath();
    }

    /**
     * @param fut Future.
     * @return {@code True} if rebalance topology version changed by exchange thread or force
     * reassing exchange occurs, see {@link RebalanceReassignExchangeTask} for details.
     */
    private boolean topologyChanged(RebalanceDownloadFuture fut) {
        return !cctx.exchange().rebalanceTopologyVersion().equals(fut.topVer);
                // todo || fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
    }

    /** */
    private static class CheckpointListener implements DbCheckpointListener {
        /** Queue. */
        private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) {
            Runnable r;

            while ((r = queue.poll()) != null)
                r.run();
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** */
        public void cancelAll() {
            queue.clear();
        }

        public IgniteInternalFuture<Void> schedule(final Runnable task) {
            return schedule(() -> {
                task.run();

                return null;
            });
        }

        public <R> IgniteInternalFuture<R> schedule(final Callable<R> task) {
            return schedule(new CheckpointTask<>(task));
        }

        private <R> IgniteInternalFuture<R> schedule(CheckpointTask<R> task) {
            queue.offer(task);

            return task.fut;
        }

        /** */
        private static class CheckpointTask<R> implements Runnable {
            /** */
            final GridFutureAdapter<R> fut = new GridFutureAdapter<>();

            /** */
            final Callable<R> task;

            /** */
            CheckpointTask(Callable<R> task) {
                this.task = task;
            }

            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    fut.onDone(task.call());
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
            }
        }
    }

    /** */
    private class RebalanceDownloadHandler implements TransmissionHandler {
        /** {@inheritDoc} */
        @Override public void onException(UUID nodeId, Throwable err) {
            // todo
        }

        /** {@inheritDoc} */
        @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
            RebalanceDownloadFuture fut = futMap.get(nodeId);

            // todo check what to return
            if (staleFuture(fut))
                return null;

            try {
                Integer grpId = (Integer)fileMeta.params().get("group");
                Integer partId = (Integer)fileMeta.params().get("part");

                assert grpId != null;
                assert partId != null;

                return storePath(grpId, partId) + ".$$$";
            } catch (IgniteCheckedException e) {
                fut.onDone(e);

                throw new IgniteException("File transfer exception.", e);
            }
        }

        /** {@inheritDoc} */
        @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
            return file -> {
                RebalanceDownloadFuture fut = futMap.get(nodeId);

                if (staleFuture(fut))
                    return;

                Integer grpId = (Integer)initMeta.params().get("group");
                Integer partId = (Integer)initMeta.params().get("part");

                IgniteInternalFuture destroyFut = fut.evictionFuture(grpId);

                try {
                    fut.switchFut(grpId, partId, new GridFutureAdapter());

                    IgniteInternalFuture<T2<Long, Long>> switchFut = restorePartition(grpId, partId, file, destroyFut);

                    switchFut.listen( f -> {
                        try {
                            T2<Long, Long> cntrs = f.get();

                            assert cntrs != null;

                            cctx.kernalContext().closure().runLocalSafe(() -> {
                                fut.onPartitionRestored(grpId, partId, cntrs.get1(), cntrs.get2());
                            });
                        } catch (IgniteCheckedException e) {
                            fut.onDone(e);
                        }
                    });
                } catch (IgniteCheckedException e) {
                    fut.onDone(e);
                }
            };
        }
    }

    /** */
    private static class RebalanceDownloadFuture extends GridFutureAdapter<Boolean> {
        /** Context. */
        protected GridCacheSharedContext cctx;

        /** Logger. */
        protected IgniteLogger log;

        /** */
        private long rebalanceId;

        /** */
        @GridToStringInclude
        private Map<Integer, Set<Integer>> assigns;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Set<Integer>> remaining;

        /** */
        private Map<Integer, Set<HistoryDesc>> remainingHist;

        /** {@code True} if the initial demand request has been sent. */
        private AtomicBoolean initReq = new AtomicBoolean();

        /** */
        private final ClusterNode node;

        /** */
        private final AtomicReference<GridFutureAdapter> switchFut = new AtomicReference<>();

        /** */
        private final Map<String, PageMemCleanupFuture> cleanupRegions = new HashMap<>();

        /** */
        private final CheckpointListener cpLsnr;

        /**
         * Default constructor for the dummy future.
         */
        public RebalanceDownloadFuture() {
            this(null, null, null, 0, Collections.emptyMap(), null, null);

            onDone();
        }

        /**
         * @param node Supplier node.
         * @param rebalanceId Rebalance id.
         * @param assigns Map of assignments to request from remote.
         * @param topVer Topology version.
         */
        public RebalanceDownloadFuture(
            GridCacheSharedContext cctx,
            IgniteLogger log,
            ClusterNode node,
            long rebalanceId,
            Map<Integer, Set<Integer>> assigns,
            AffinityTopologyVersion topVer,
            CheckpointListener cpLsnr
        ) {
            this.cctx = cctx;
            this.log = log;
            this.node = node;
            this.rebalanceId = rebalanceId;
            this.assigns = assigns;
            this.topVer = topVer;
            this.cpLsnr = cpLsnr;

            remaining = new ConcurrentHashMap<>(assigns.size());
            remainingHist = new ConcurrentHashMap<>(assigns.size());

            Map<String, Set<Long>> regionToParts = new HashMap<>();

            for (Map.Entry<Integer, Set<Integer>> entry : assigns.entrySet()) {
                String regName = cctx.cache().cacheGroup(entry.getKey()).dataRegion().config().getName();
                Set<Integer> parts = entry.getValue();
                int partsCnt = parts.size();
                int grpId = entry.getKey();

                assert !remaining.containsKey(grpId);

                Set<Integer> parts0 = new GridConcurrentHashSet<>(partsCnt);

                remaining.put(grpId, parts0);

                Set<Long> regionParts = regionToParts.computeIfAbsent(regName, v -> new HashSet<>());

                for (Integer partId : entry.getValue()) {
                    regionParts.add(((long)grpId << 32) + partId);

                    parts0.add(partId);
                }
            }

            for (Map.Entry<String, Set<Long>> e : regionToParts.entrySet())
                cleanupRegions.put(e.getKey(), new PageMemCleanupFuture(e.getKey(), e.getValue()));
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            cpLsnr.cancelAll();

            return onCancelled();
        }

        /**
         * @param grpId Cache group id to search.
         * @param partId Cache partition to remove;
         */
        public void onPartitionRestored(int grpId, int partId, long min, long max) {
            Set<Integer> parts = remaining.get(grpId);

            assert parts != null : "Invalid group identifier: " + grpId;

            boolean rmvd = parts.remove(partId);

            assert rmvd : "Partition not found: " + partId;

            remainingHist.computeIfAbsent(grpId, v -> new ConcurrentSkipListSet<>())
                .add(new HistoryDesc(partId, min, max));

            GridFutureAdapter fut0 = switchFut.get();

            if (parts.isEmpty() && switchFut.compareAndSet(fut0, null)) {
                fut0.onDone();

                remaining.remove(grpId);

                Set<HistoryDesc> parts0 = remainingHist.remove(grpId);

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grpId);

                for (HistoryDesc desc : parts0) {
                    if (log.isDebugEnabled()) {
                        log.debug("Prepare to request historical rebalancing [p=" +
                            desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");
                    }

                    msg.partitions().addHistorical(desc.partId, desc.fromCntr, desc.toCntr, parts0.size());
                }

                GridDhtPartitionExchangeId exchId = cctx.exchange().lastFinishedFuture().exchangeId();

                GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchId, topVer);

                assigns.put(node, msg);

                GridCompoundFuture<Boolean, Boolean> forceFut = new GridCompoundFuture<>(CU.boolReducer());

                Runnable cur = grp.preloader().addAssignments(assigns,
                    true,
                    rebalanceId,
                    null,
                    forceFut);

                if (log.isDebugEnabled())
                    log.debug("Triggering historical rebalancing [node=" + node.id() + ", group=" + grp.cacheOrGroupName() + "]");

                cur.run();

                forceFut.markInitialized();

                forceFut.listen(c -> {
                    try {
                        if (forceFut.get() && remaining.isEmpty())
                            onDone(true);
                        else
                            cancel();
                    }
                    catch (IgniteCheckedException e) {
                        onDone(e);
                    }
                });
            }
        }

        public void onPartitionEvicted(int grpId, int partId) throws IgniteCheckedException {
            CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

            String regName = gctx.dataRegion().config().getName();

            PageMemCleanupFuture fut = cleanupRegions.get(regName);

            fut.onPartitionEvicted();
        }

        public void switchFut(int grpId, int partId, GridFutureAdapter waitFUt) {
            switchFut.compareAndSet(null, waitFUt);
        }

        public IgniteInternalFuture switchFut(int grpId, int partId) {
            return switchFut.get();
        }

        public IgniteInternalFuture evictionFuture(int grpId) {
            String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

            return cleanupRegions.get(regName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceDownloadFuture.class, this);
        }

        private static class HistoryDesc implements Comparable {
            /** Partition id. */
            final int partId;

            /** From counter. */
            final long fromCntr;

            /** To counter. */
            final long toCntr;

            public HistoryDesc(int partId, long fromCntr, long toCntr) {
                this.partId = partId;
                this.fromCntr = fromCntr;
                this.toCntr = toCntr;
            }

            @Override public int compareTo(@NotNull Object o) {
                HistoryDesc otherDesc = (HistoryDesc)o;

                if (partId > otherDesc.partId)
                    return 1;

                if (partId < otherDesc.partId)
                    return -1;

                return 0;
            }
        }

        private class PageMemCleanupFuture extends GridFutureAdapter {
            private final Set<Long> parts;
            private final AtomicInteger evictedCntr;
            private final String name;

            public PageMemCleanupFuture(String regName, Set<Long> remainingParts) {
                name = regName;
                parts = remainingParts;
                evictedCntr = new AtomicInteger();
            }

            public void onPartitionEvicted() throws IgniteCheckedException {
                int evictedCnt = evictedCntr.incrementAndGet();

                assert evictedCnt <= parts.size();

                if (evictedCnt == parts.size()) {
                    ((PageMemoryEx)cctx.database().dataRegion(name).pageMemory())
                        .clearAsync(
                            (grp, pageId) ->
                                parts.contains(((long)grp << 32) + PageIdUtils.partId(pageId)), true)
                        .listen(c1 -> {
                            if (log.isDebugEnabled())
                                log.debug("Eviction is done [region=" + name + "]");

                            onDone();
                        });
                }
            }
        }
    }
}
