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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
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
public class GridCachePreloadSharedManager extends GridCacheSharedManagerAdapter implements DbCheckpointListener {
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

    /** */
    private final ConcurrentMap<UUID, RebalanceDownloadFuture> futMap = new ConcurrentHashMap<>();

    /** */
    private volatile RebalanceDownloadFuture headFut = new RebalanceDownloadFuture();

    /** */
    private PartitionUploadManager uploadMgr;

    /** */
    private final ConcurrentLinkedQueue<Runnable> checkpointReqs = new ConcurrentLinkedQueue<>();

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
    static boolean staleFuture(GridFutureAdapter<?> fut) {
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

        ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(this);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            uploadMgr.stop0(cancel);

            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(this);

            for (RebalanceDownloadFuture rebFut : futMap.values())
                rebFut.cancel();

            futMap.clear();

            // todo
//            cctx.gridIO().removeChannelListener(rebalanceThreadTopic(), null);
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

            if (cctx.preloader().partitionRebalanceRequired(grp, grpEntry.getValue())) {
                int grpOrderNo = grp.config().getRebalanceOrder();

                result.putIfAbsent(grpOrderNo, new HashMap<>());

                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> grpAssigns : grpEntry.getValue().entrySet()) {
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

        // TODO Start eviction.
        // Assume that the partition tag will be changed on eviction process finished,
        // so we will have no additional writes (via writeInternal method) to current
        // MOVING partition if checkpoint thread occures. So the current partition file
        // can be easily replaced with the new one received from the socket.

        lock.writeLock().lock();

        try {
            RebalanceDownloadFuture rqFut = null;
            Runnable rq = NO_OP;

            U.log(log, "Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            // Clear the previous rebalance futures if exists.
            futMap.clear();

            for (Map<ClusterNode, Map<Integer, Set<Integer>>> descNodeMap : nodeOrderAssignsMap.descendingMap().values()) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> assignEntry : descNodeMap.entrySet()) {
                    RebalanceDownloadFuture rebFut = new RebalanceDownloadFuture(assignEntry.getKey(), rebalanceId,
                        assignEntry.getValue(), topVer);

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

                U.log(log, "Start partitions preloading [from=" + node.id() + ", fut=" + rebFut + ']');

                final Map<Integer, Set<Integer>> assigns = rebFut.nodeAssigns;

                IgniteInternalFuture<Void> switchFut = cctx.preloader().offerCheckpointTask(() -> {
                    log.info("switching partitions");

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

                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    for (Integer partId : e.getValue()) {
                        GridDhtLocalPartition part = grp.topology().localPartition(partId);

                        log.debug("Add destroy future for partition " + part.id());

                        rebFut.remainDestroy(grpId, partId, destroyPartition(part));
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
     * @return {@code True} if rebalance via sending partitions files enabled. Default <tt>false</tt>.
     */
    public boolean isPresistenceRebalanceEnabled() {
        return presistenceRebalanceEnabled;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        if (assigns == null || assigns.isEmpty())
            return false;

        return rebalanceByPartitionSupported(grp, assigns.keySet());
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param topVer Topology versions to calculate assignmets at.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, AffinityTopologyVersion topVer) {
        AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

        // All of affinity nodes must support to new persistence rebalance feature.
        List<ClusterNode> affNodes =  aff.idealAssignment().stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());

        return rebalanceByPartitionSupported(grp, affNodes);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param nodes The list of nodes to check ability of file transferring.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    private boolean rebalanceByPartitionSupported(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        if (grp.mvccEnabled())
            return false;

        return presistenceRebalanceEnabled &&
            grp.persistenceEnabled() &&
            IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean partitionRebalanceRequired(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        return rebalanceByPartitionSupported(grp, assigns) &&
            grp.config().getRebalanceDelay() != -1 &&
            grp.config().getRebalanceMode() != CacheRebalanceMode.NONE;
    }

    /**
     * @return The instantiated upload mamanger.
     */
    public PartitionUploadManager upload() {
        return uploadMgr;
    }

    /**
     * @param fut Exchange future.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture fut) {
        // todo switch to read-only mode after first exchange
        System.out.println(">xxx> process");
    }

//    static AtomicLong rebalanceIdCntr = new AtomicLong(100);

    public void triggerHistoricalRebalance(ClusterNode node, GridCacheContext cctx, int[] p, long[] lwm, long[] hwm, int partsCnt, long rebalanceId) {
        GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(
            cctx.topology().updateSequence(),
            cctx.topology().readyTopologyVersion(),
            cctx.groupId());

        for (int i = 0; i < p.length; i++)
            msg.partitions().addHistorical(p[i], lwm[i], hwm[i], partsCnt);

        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        GridDhtPartitionExchangeId exchId = cctx.shared().exchange().lastFinishedFuture().exchangeId();

        GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchId, cctx.topology().readyTopologyVersion());

        assigns.put(node, msg);

        Runnable cur = cctx.group().preloader().addAssignments(assigns,
            true,
            rebalanceId,
            null,
            fut);

        cur.run();
    }

    private void triggerHistoricalRebalance0(ClusterNode node, GridCacheContext cctx, int[] p, long[] lwm, long[] hwm, int partsCnt) {
        GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(
            cctx.topology().updateSequence(),
            cctx.topology().readyTopologyVersion(),
            cctx.groupId());

        for (int i = 0; i < p.length; i++)
            msg.partitions().addHistorical(p[i], lwm[i], hwm[i], partsCnt);

        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        GridDhtPartitionExchangeId exchId = cctx.shared().exchange().lastFinishedFuture().exchangeId();

        GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchId, cctx.topology().readyTopologyVersion());

        assigns.put(node, msg);

        Runnable cur = cctx.group().preloader().addAssignments(assigns,
            true,
            headFut.rebalanceId,
            null,
            fut);

        cur.run();
    }

    public IgniteInternalFuture<Void> schedulePartitionDestroy(GridDhtLocalPartition part) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        offerCheckpointTask(
            () -> part.readOnly(true)
        ).listen(
            c -> destroyPartition(part)
                .listen(
                    c0 -> {
                        fut.onDone();
                    }
                )
        );

        return fut;
    }

    // todo destroy partition but don't change state in node2part map
    private IgniteInternalFuture<Void> destroyPartition(GridDhtLocalPartition part) {
        GridFutureAdapter<Void> destroyFut = new GridFutureAdapter<>();

        part.clearAsync();

        part.onClearFinished(c -> {
            cctx.kernalContext().closure().runLocalSafe(() -> {
                //todo should prevent any removes on DESTROYED partition.
                ReadOnlyGridCacheDataStore store = (ReadOnlyGridCacheDataStore)part.dataStore().store(true);

                store.disableRemoves();

                CacheGroupContext ctx = part.group();

                int grpId = ctx.groupId();

                try {
                    ctx.offheap().destroyCacheDataStore(part.dataStore());

                    // todo something smarter - store will be removed on next checkpoint.
                    while (ctx.shared().pageStore().exists(grpId, part.id()))
                        U.sleep(200);

                    // todo should be executed for all cleared partitions at once.
                    ((PageMemoryEx)ctx.dataRegion().pageMemory())
                        .clearAsync((grp, pageId) -> grp == grpId && part.id() == PageIdUtils.partId(pageId), true)
                        .listen(c1 -> {
                            if (log.isDebugEnabled())
                                log.debug("Eviction is done [grp=" + grpId + ", part=" + part.id() + "]");

                            destroyFut.onDone();
                        });
                }
                catch (IgniteCheckedException e) {
                    destroyFut.onDone(e);
                }
            });
        });

        return destroyFut;
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

        return offerCheckpointTask(() -> {
            // Create partition.
            GridDhtLocalPartition part = ctx.topology().forceCreatePartition(partId);

            PartitionUpdateCounter readOnlyCntr = part.dataStore().partUpdateCounter();

            readOnlyCntr.finalizeUpdateCounters();

            part.dataStore().store(false).reinit();

            // todo clean-up heap entries
            part.entriesMap(null).map.clear();

            part.readOnly(false);

//            System.out.println("after restore p="+partId+", reserved="+part.reservedCounter()+", cntr="+part.updateCounter() +" to="+to);

            return new T2<>(part.updateCounter(), readOnlyCntr.get());
        });
    }

    // todo protected
    public IgniteInternalFuture<Void> offerCheckpointTask(final Runnable task) {
        return offerCheckpointTask(() -> {
           task.run();

           return null;
        });
    }

    private <R> IgniteInternalFuture<R> offerCheckpointTask(final Callable<R> task) {
        class CheckpointTask implements Runnable {
            final GridFutureAdapter<R> fut = new GridFutureAdapter<>();

            final Callable<R> task;

            CheckpointTask(Callable<R> task) {
                this.task = task;
            }

            @Override public void run() {
                try {
                    fut.onDone(task.call());
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
            }
        }

        CheckpointTask task0 = new CheckpointTask(task);

        checkpointReqs.offer(task0);

        return task0.fut;
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        Runnable r;

        while ((r = checkpointReqs.poll()) != null)
            r.run();
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {

    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {

    }

    public void handleDemandMessage(UUID nodeId, GridPartitionBatchDemandMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Handling demand request " + msg.rebalanceId());

        if (msg.rebalanceId() < 0) // Demand node requested context cleanup.
            return;

        ClusterNode demanderNode = cctx.discovery().node(nodeId);

        if (demanderNode == null) {
            U.error(log, "The demand message rejected (demander node left the cluster) ["
                + ", nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']');

            return;
        }

        if (msg.assignments() == null || msg.assignments().isEmpty()) {
            U.error(log, "The Demand message rejected. Node assignments cannot be empty ["
                + "nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']');

            return;
        }

//        IgniteSocketChannel ch = null;
//        CachePartitionUploadFuture uploadFut = null;

        uploadMgr.onDemandMessage(nodeId, msg, PUBLIC_POOL);
    }

    public String storePath(int grpId, int partId) throws IgniteCheckedException {
        return ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId)).getFileAbsolutePath();
    }

    public IgniteInternalFuture partitionRestoreFuture(GridCacheMessage msg) {
        if (futMap.isEmpty())
            return null;

        if (!(msg instanceof GridCacheIdMessage) && !(msg instanceof GridCacheGroupIdMessage)) {
            log.warning("Skipping message: " + msg.getClass().getSimpleName());

            return null;
        }

        assert futMap.size() == 1 : futMap.size();

        RebalanceDownloadFuture rebFut = futMap.values().iterator().next();

        return rebFut.switchFut();
    }

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

//                try {
                    IgniteInternalFuture destroyFut = fut.remainDestroy(grpId, partId);

//                    Executor exec = cctx.kernalContext().pools().poolForPolicy(PUBLIC_POOL);
                    try {

                        GridFutureAdapter waitFUt = new GridFutureAdapter();

                        fut.switchFut(waitFUt);

                        // Await while all operations complete.
//                        U.sleep(1_000);

                        IgniteInternalFuture<T2<Long, Long>> switchFut = restorePartition(grpId, partId, file, destroyFut);

                        T2<Long, Long> cntrs = switchFut.get();

                        assert cntrs != null;

                        waitFUt.onDone();

//                        cctx.kernalContext().closure().runLocalSafe( () -> {
                        fut.onPartitionRestored(grpId, partId, cntrs.get1(), cntrs.get2());


//                        });

                    } catch (IgniteCheckedException e) {
                        fut.onDone(e);
                    }

//                    return true;
//                }
//                catch (IgniteCheckedException e) {
//                    fut.onDone(e);
//
//                    throw new IgniteException("File transfer exception.", e);
//                }
            };
        }
    }

    /** */
    private class RebalanceDownloadFuture extends GridFutureAdapter<Boolean> {
        private final ClusterNode node;

        private volatile IgniteInternalFuture switchFut;

        public IgniteInternalFuture switchFut() {
            return switchFut;
        }

        public void switchFut(IgniteInternalFuture switchFut) {
            this.switchFut = switchFut;
        }

        class HistoryDesc implements Comparable {
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

        /** */
        private UUID nodeId;

        /** */
        private long rebalanceId;

        /** */
        @GridToStringInclude
        private Map<Integer, Set<Integer>> nodeAssigns;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Set<Integer>> remaining;

        /** */
        private Map<Integer, Set<HistoryDesc>> remainingHist;

        /** {@code True} if the initial demand request has been sent. */
        private AtomicBoolean initReq = new AtomicBoolean();

        /**
         * Default constructor for the dummy future.
         */
        public RebalanceDownloadFuture() {
            this(null, 0, Collections.emptyMap(), null);

            onDone();
        }

        /**
         * @param nodeId The remote nodeId.
         * @param nodeAssigns Map of assignments to request from remote.
         */
        public RebalanceDownloadFuture(
            ClusterNode node,
            long rebalanceId,
            Map<Integer, Set<Integer>> nodeAssigns,
            AffinityTopologyVersion topVer
        ) {
            this.node = node;
            this.nodeId = nodeId;
            this.rebalanceId = rebalanceId;
            this.nodeAssigns = nodeAssigns;
            this.topVer = topVer;

            remaining = U.newHashMap(nodeAssigns.size());
            remainingHist = U.newHashMap(nodeAssigns.size());

            for (Map.Entry<Integer, Set<Integer>> grpPartEntry : nodeAssigns.entrySet())
                remaining.putIfAbsent(grpPartEntry.getKey(), new HashSet<>(grpPartEntry.getValue()));
        }

        Map<Long, IgniteInternalFuture> remainDestroy = new HashMap<>();

        void remainDestroy(int grpId, int partId, IgniteInternalFuture fut) {
            remainDestroy.put(((long)grpId << 32) + partId, fut);
        }

        IgniteInternalFuture remainDestroy(int grpId, int partId) {
            return remainDestroy.get(((long)grpId << 32) + partId);
        }


        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

//        /** */
//        public synchronized void onCompleteSuccess() {
//            assert remaining.isEmpty();
//
//            U.log(log, "Partitions have been scheduled to resend. Files have been transferred " +
//                "[from=" + nodeId + ", to=" + cctx.localNodeId() + ']');
//
//            // Late affinity assignment
//            cctx.exchange().scheduleResendPartitions();
//
//            onDone(true);
//        }

        /**
         * @param grpId Cache group id to search.
         * @param partId Cache partition to remove;
         */
        public synchronized void onPartitionRestored(int grpId, int partId, long startCntr, long endCntr) {
            Set<Integer> parts = remaining.get(grpId);

            if (parts == null)
                onDone(new IgniteCheckedException("Partition index incorrect [grpId=" + grpId + ", partId=" + partId + ']'));

            boolean success = parts.remove(partId);

            assert success : "Partition not found: " + partId;

            CacheGroupContext ctx = cctx.cache().cacheGroup(grpId);

            boolean locWalEnabled = ctx.localWalEnabled();

            if (locWalEnabled) {
                if (log.isDebugEnabled())
                    log.debug("Owning partition [cache=" + ctx.cacheOrGroupName() + ", part=" + partId);

                boolean isOwned = ctx.topology().own(ctx.topology().localPartition(partId));

                assert isOwned : "Partition must be owned: " + partId;
            }

            remainingHist.computeIfAbsent(grpId, v -> new TreeSet<>()).add(new HistoryDesc(partId, startCntr, endCntr));

            if (parts.isEmpty()) {
                try {
                    U.sleep(1_000);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }

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

//        // todo when historical rebalancing finishes.
//        public void onPartitionDone(int grpId, int partId) {
//
//        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceDownloadFuture.class, this);
        }
    }
}
