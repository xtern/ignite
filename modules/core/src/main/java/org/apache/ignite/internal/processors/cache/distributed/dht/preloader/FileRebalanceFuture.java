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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

public class FileRebalanceFuture extends GridFutureAdapter<Boolean> {
    /** */
    private final Map<T2<Integer, UUID>, FileRebalanceNodeFuture> futs = new HashMap<>();

    /** */
    private final GridCachePreloadSharedManager.CheckpointListener cpLsnr;

    /** */
    private final Map<Integer, Set<Integer>> allPartsMap = new HashMap<>();

    /** */
    private final Map<Integer, Set<UUID>> allGroupsMap = new ConcurrentHashMap<>();

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final long rebalanceId;

    /** */
    private final Map<String, PageMemCleanupTask> regions = new HashMap<>();

    /** */
    private final ReentrantLock cancelLock = new ReentrantLock();

    /** */
    private final GridCacheSharedContext cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final Map<Integer, GridDhtPreloaderAssignments> historicalAssignments = new ConcurrentHashMap<>();

    /** */
    public FileRebalanceFuture() {
        this(null, null, null, null, 0, null);

        onDone(true);
    }

    /**
     * @param lsnr Checkpoint listener.
     */
    public FileRebalanceFuture(
        GridCachePreloadSharedManager.CheckpointListener lsnr,
        NavigableMap</** order */Integer, Map<ClusterNode, Map</** group */Integer, Set</** part */Integer>>>> assignsMap,
        AffinityTopologyVersion startVer,
        GridCacheSharedContext cctx,
        long rebalanceId,
        IgniteLogger log
    ) {
        cpLsnr = lsnr;
        topVer = startVer;

        this.log = log;
        this.cctx = cctx;
        this.rebalanceId = rebalanceId;

        // The dummy future does not require initialization.
        if (assignsMap != null)
            initialize(assignsMap);
    }

    public boolean isPreloading(int grpId) {
        return allGroupsMap.containsKey(grpId) && !isDone();
    }

    /**
     * Initialize rebalancing mappings.
     *
     * @param assignments Assignments.
     */
    private synchronized void initialize(NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> assignments) {
        assert assignments != null;
        assert !assignments.isEmpty();

        Map<String, Set<Long>> regionToParts = new HashMap<>();

        // todo redundant?
        cancelLock.lock();

        try {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : assignments.values()) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> mapEntry : map.entrySet()) {
                    UUID nodeId = mapEntry.getKey().id();

                    for (Map.Entry<Integer, Set<Integer>> entry : mapEntry.getValue().entrySet()) {
                        int grpId = entry.getKey();

                        allGroupsMap.computeIfAbsent(grpId, v -> new GridConcurrentHashSet<>()).add(nodeId);

                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        // todo remove
                        //assert cctx.filePreloader().fileRebalanceRequired(grp, assigns, exchFut);

                        String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

                        Set<Long> regionParts = regionToParts.computeIfAbsent(regName, v -> new HashSet<>());

                        Set<Integer> allPartitions = allPartsMap.computeIfAbsent(grpId, v -> new HashSet<>());

//                        GridDhtPartitionDemandMessage msg = e.getValue();
//                        ClusterNode node = e.getKey();
//
//                        nodes.add(node.id());

//                        Set<Integer> parttitions = msg.partitions().fullSet();

                        for (Integer partId : entry.getValue()) {
                            assert grp.topology().localPartition(partId).dataStore().readOnly() :
                                "cache=" + grp.cacheOrGroupName() + " p=" + partId;

                            regionParts.add(((long)grpId << 32) + partId);

                            allPartitions.add(partId);
                        }
                    }
                }
            }

            //for (Map.Entry<Integer, GridDhtPreloaderAssignments> entry : assignments.entrySet()) {

//            }

            for (Map.Entry<String, Set<Long>> e : regionToParts.entrySet())
                regions.put(e.getKey(), new PageMemCleanupTask(e.getKey(), e.getValue()));
        }
        finally {
            cancelLock.unlock();
        }
    }

    /** */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    public synchronized void add(int order, FileRebalanceNodeFuture fut) {
        T2<Integer, UUID> k = new T2<>(order, fut.nodeId());

        futs.put(k, fut);
    }

    // todo add/get should be consistent (ORDER or GROUP_ID arg)
    public synchronized FileRebalanceNodeFuture nodeRoutine(int grpId, UUID nodeId) {
        int order = cctx.cache().cacheGroup(grpId).config().getRebalanceOrder();

        T2<Integer, UUID> k = new T2<>(order, nodeId);

        return futs.get(k);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        if (cancel || err != null) {
            cancelLock.lock();

            try {
                synchronized (this) {
                    if (isDone())
                        return true;

                    if (log.isInfoEnabled())
                        log.info("Cancelling file rebalancing.");

                    cpLsnr.cancelAll();

                    for (IgniteInternalFuture fut : regions.values()) {
                        if (!fut.isDone())
                            fut.cancel();
                    }

                    // todo eliminate ConcurrentModification
                    for (FileRebalanceNodeFuture fut : new HashMap<>(futs).values()) {
                        if (!fut.isDone())
                            fut.cancel();
                    }

                    futs.clear();
                }
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
            finally {
                cancelLock.unlock();
            }
        }

        return super.onDone(res, err, cancel);
    }

    public void onCacheGroupDone(int grpId, UUID nodeId, GridDhtPartitionDemandMessage msg) {
        Set<UUID> remainingNodes = allGroupsMap.get(grpId);

        boolean rmvd = remainingNodes.remove(nodeId);

        assert rmvd : "Duplicate remove " + nodeId;

        if (msg.partitions().hasHistorical()) {
            GridDhtPartitionExchangeId exchId = cctx.exchange().lastFinishedFuture().exchangeId();

            historicalAssignments.computeIfAbsent(grpId, v -> new GridDhtPreloaderAssignments(exchId, topVer)).put(cctx.discovery().node(nodeId), msg);
        }

        if (remainingNodes.isEmpty() && allGroupsMap.remove(grpId) != null) {
            GridDhtPreloaderAssignments assigns = historicalAssignments.remove(grpId);

            if (assigns != null) {
                GridCompoundFuture<Boolean, Boolean> histFut = new GridCompoundFuture<>(CU.boolReducer());

                Runnable task = cctx.cache().cacheGroup(grpId).preloader().addAssignments(assigns, true, rebalanceId, null, histFut);

                // todo do we need to run it async
                cctx.kernalContext().getSystemExecutorService().submit(task);

                return;
            }

            CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

            log.info("Rebalancing complete [group=" + gctx.cacheOrGroupName() + "]");

            if (gctx.localWalEnabled())
                cctx.exchange().scheduleResendPartitions();
            else
                cctx.walState().onGroupRebalanceFinished(gctx.groupId(), topVer);
        }
    }

    public synchronized void onNodeDone(FileRebalanceNodeFuture fut, Boolean res, Throwable err, boolean cancel) {
        if (err != null || cancel) {
            onDone(res, err, cancel);

            return;
        }

        GridFutureAdapter<Boolean> rmvdFut = futs.remove(new T2<>(fut.order(), fut.nodeId()));

        assert rmvdFut != null && rmvdFut.isDone() : rmvdFut;

//        if (futs.isEmpty()) {
//            histFut.listen(c -> {
//                onDone(true);
//            });
//        }

        if (futs.isEmpty()) {
            onDone(true);
        }
    }

    /**
     * Switch all rebalanced partitions to read-only mode and start evicting.
     */
    public void clearPartitions() {
        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Cancelling clear and invalidation");

            return;
        }

//        cancelLock.lock();

        for (Map.Entry<Integer, Set<Integer>> e : allPartsMap.entrySet()) {
            int grpId = e.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            if (log.isDebugEnabled())
                log.debug("Clearing partitions [grp=" + grp.cacheOrGroupName() + "]");

            for (Integer partId : e.getValue()) {
                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                log.info("clearAsync p=" + partId + " cache=" + grp.cacheOrGroupName() + ", topVer=" + topVer);

                PageMemCleanupTask task = regions.get(grp.dataRegion().config().getName());

                cancelLock.lock();

                try {
                    assert !part.isClearing() : "Partition is cleared currently: " + part.id();

                    task.onPartitionCleared(grp.topology().localPartition(partId));
                }
                catch (AssertionError | IgniteCheckedException ex) {
                    log.error("Unable to finish partition cleanup.", ex);

                    task.onDone(ex);

                    onDone(ex);
                } finally {
                    cancelLock.unlock();
                }

//                part.clearAsync();
//
//                part.onClearFinished(c -> {
//                    log.info("onClearAsync finished p=" + partId + " cache=" + grp.cacheOrGroupName() + ", topVer=" + topVer);
//                    cancelLock.lock();
//
//                    try {
//                        if (isDone()) {
//                            if (log.isDebugEnabled()) {
//                                log.debug("Page memory cleanup canceled [grp=" + grp.cacheOrGroupName() +
//                                    ", p=" + partId + ", topVer=" + topVer + "]");
//                            }
//
//                            return;
//                        }
//
//                        PageMemCleanupTask task = regions.get(grp.dataRegion().config().getName());
//
//                        if (log.isDebugEnabled())
//                            log.debug("OnPartitionCleared [topVer=" + topVer + "]");
//
//                        task.onPartitionCleared();
//                    }
//                    catch (IgniteCheckedException ex) {
//                        onDone(ex);
//                    }
//                    finally {
//                        cancelLock.unlock();
//                    }
//                });
            }
        }
    }

    /**
     * Wait for region cleaning if necessary.
     *
     * @param grpId Group ID.
     * @throws IgniteCheckedException If the cleanup failed.
     */
    public void awaitCleanupIfNeeded(int grpId) throws IgniteCheckedException {
        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        IgniteInternalFuture fut = regions.get(grp.dataRegion().config().getName());

        if (fut.isCancelled()) {
            log.info("The cleaning task has been canceled.");

            return;
        }

        if (!fut.isDone() && log.isDebugEnabled())
            log.debug("Wait cleanup [grp=" + grp + "]");

        try {
            fut.get();
        } catch (IgniteFutureCancelledCheckedException ignore) {
            // No-op.
        }
    }

    private class PageMemCleanupTask extends GridFutureAdapter {
        private final Set<Long> parts;

        private final AtomicInteger evictedCntr;

        private final String name;

        public PageMemCleanupTask(String regName, Set<Long> remainingParts) {
            name = regName;
            parts = remainingParts;
            evictedCntr = new AtomicInteger();
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            //return onDone(null, null, true);
            System.out.println("fake cancel - will wait cleanup " + name);

            try {
                get();
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }

            return false;
        }

        public void onPartitionCleared(GridDhtLocalPartition partition) throws IgniteCheckedException {
            if (isCancelled())
                return;

            int evictedCnt = evictedCntr.incrementAndGet();

            assert evictedCnt <= parts.size();

            if (log.isDebugEnabled())
                log.debug("Partition cleared [remain=" + (parts.size() - evictedCnt) + "]");

            partition.reserve();

            if (evictedCnt == parts.size()) {
                DataRegion region = cctx.database().dataRegion(name);

                if (isCancelled())
                    return;

                PageMemoryEx memEx = (PageMemoryEx)region.pageMemory();

                if (log.isDebugEnabled())
                    log.debug("Cleaning up region " + name);

                memEx.clearAsync(
                    (grp, pageId) -> {
//                                if (isCancelled())
//                                    return false;

                        return parts.contains(((long)grp << 32) + PageIdUtils.partId(pageId));
                    }, true)
                    .listen(c1 -> {
                        // todo misleading should be reformulate
//                            cctx.database().checkpointReadLock();
                        cctx.database().checkpointReadLock();
//                        cancelLock.lock();

                        try {
//                            try {
                            if (log.isDebugEnabled())
                                log.debug("Off heap region cleared [node=" + cctx.localNodeId() + ", region=" + name + "]");

                            for (long partGrp : parts) {
                                int grpId = (int)(partGrp >> 32);
                                int partId = (int)partGrp;

                                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                                int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grpId, partId);

                                ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId).truncate(tag);

                                if (log.isDebugEnabled())
                                    log.debug("Parition truncated [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId + "]");

                                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                                ((GridCacheOffheapManager.GridCacheDataStore)part.dataStore().store(false)).close();

                                part.release();
                            }

                            onDone();
                        } catch (IgniteCheckedException e) {
                            onDone(e);

                            FileRebalanceFuture.this.onDone(e);
                        }
                        finally {
//                            cancelLock.unlock();

                                cctx.database().checkpointReadUnlock();
                        }
                    });

                if (!isDone()) {
                    log.info("Wait for cleanup region " + region.config().getName());

                    get();

                    log.info("Done wait for cleanup region " + region.config().getName());
                }
//                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "finished=" + isDone() + ", region=" + name + ", total=" + parts.size() + ", remain=" + (parts.size() - evictedCntr.get());
        }
    }

    // todo
    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("\n\tNode routines:\n");

        for (FileRebalanceNodeFuture fut : futs.values())
            buf.append("\t\t" + fut.toString() + "\n");

        buf.append("\n\tMemory regions:\n");

        for (PageMemCleanupTask entry : regions.values())
            buf.append("\t\t" + entry.toString() + "\n");

        return buf.toString();
    }
}