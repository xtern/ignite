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
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;

/**
 * File rebalance routine.
 */
public class FileRebalanceRoutine extends GridFutureAdapter<Boolean> {
    /** */
    private static final long MAX_MEM_CLEANUP_TIMEOUT = 60_000;

    /** Cancel callback invoked when routine is aborted. */
    private final IgniteRunnable abortCb;

    /** Rebalance topology version. */
    private final AffinityTopologyVersion topVer;

    /** Unique (per demander) rebalance id. */
    private final long rebalanceId;

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Cache context. */
    private final GridCacheSharedContext cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Index rebuild future. */
    private final GridCompoundFuture idxRebuildFut = new GridCompoundFuture<>();

    /** Exchange ID. */
    private final GridDhtPartitionExchangeId exchId;

    /** Assignments ordered by cache rebalance pririty and node. */
    private final Collection<Map<ClusterNode, Map<Integer, Set<Integer>>>> orderedAssgnments;

    /** Unique partition identifier with node identifier. */
    private final Map<Long, UUID> partsToNodes = new HashMap<>();

    /** The remaining groups with the number of partitions. */
    private final Map<Integer, Integer> remaining = new ConcurrentHashMap<>();

    /** Count of partition snapshots received. */
    private final AtomicInteger receivedCnt = new AtomicInteger();

    /** Cache group with restored partition snapshots and HWM value of update counter. */
    private final Map<Integer, Map<Integer, Long>> restored = new ConcurrentHashMap<>();

    /** Off-heap region clear tasks. */
    private final Map<String, IgniteInternalFuture> offheapClearTasks = new ConcurrentHashMap<>();

    /** Snapshot future. */
    private volatile IgniteInternalFuture<Boolean> snapFut;

    /** Initialization latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** */
    public FileRebalanceRoutine() {
        this(null, null, null, null, 0, null);

        onDone(true);
    }

    /**
     * @param assigns Assigns.
     * @param startVer Topology version on which the rebalance started.
     * @param cctx Cache shared context.
     * @param exchId Exchange ID.
     * @param rebalanceId Rebalance ID.
     * @param abortCb Abort callback to handle the abortion process from the outside.
     */
    public FileRebalanceRoutine(
        Collection<Map<ClusterNode, Map<Integer, Set<Integer>>>> assigns,
        AffinityTopologyVersion startVer,
        GridCacheSharedContext cctx,
        GridDhtPartitionExchangeId exchId,
        long rebalanceId,
        IgniteRunnable abortCb
    ) {
        this.cctx = cctx;
        this.rebalanceId = rebalanceId;
        this.exchId = exchId;
        this.abortCb = abortCb;

        orderedAssgnments = assigns;
        topVer = startVer;
        log = cctx == null ? null : cctx.logger(this.getClass());
    }

    /**
     * Initialize rebalancing mappings.
     */
    public void initialize() {
        final Map<String, Set<Long>> regionToParts = new HashMap<>();

        lock.lock();

        try {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : orderedAssgnments) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> mapEntry : map.entrySet()) {
                    UUID nodeId = mapEntry.getKey().id();

                    for (Map.Entry<Integer, Set<Integer>> entry : mapEntry.getValue().entrySet()) {
                        int grpId = entry.getKey();

                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

                        Set<Long> regionParts = regionToParts.computeIfAbsent(regName, v -> new LinkedHashSet<>());

                        for (Integer partId : entry.getValue()) {
                            assert grp.topology().localPartition(partId).dataStore().readOnly() :
                                "cache=" + grp.cacheOrGroupName() + " p=" + partId;

                            long grpAndPart = ((long)grpId << 32) + partId;

                            regionParts.add(grpAndPart);

                            partsToNodes.put(grpAndPart, nodeId);
                        }

                        Integer remainParts = remaining.get(grpId);

                        if (remainParts == null)
                            remainParts = 0;

                        remaining.put(grpId, remainParts + entry.getValue().size());
                    }
                }
            }

            if (isDone())
                return;

            for (Map.Entry<String, Set<Long>> e : regionToParts.entrySet()) {
                String regionName = e.getKey();
                Set<Long> parts = e.getValue();

                DataRegion region = cctx.database().dataRegion(regionName);

                ClearRegionTask offheapClearTask = new ClearRegionTask(parts, region, cctx, log);

                offheapClearTasks.put(regionName, offheapClearTask.doClear());
            }
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
        finally {
            lock.unlock();

            initLatch.countDown();
        }
    }

    /**
     * Request snapshot.
     */
    public void requestPartitionsSnapshot() {
        U.awaitQuiet(initLatch);

        if (isDone())
            return;

        // todo should send start event only when we starting to preload specified group?
        for (Integer grpId : remaining.keySet()) {
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            assert !grp.localWalEnabled() :
                "WAL shoud be disabled for file rebalancing [grp=" + grp.cacheOrGroupName() + "]";

            grp.preloader().sendRebalanceStartedEvent(exchId.discoveryEvent());
        }

        cctx.kernalContext().getSystemExecutorService().submit(() -> {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : orderedAssgnments) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> nodeAssigns : map.entrySet()) {
                    UUID nodeId = nodeAssigns.getKey().id();
                    Map<Integer, Set<Integer>> assigns = nodeAssigns.getValue();

                    try {
                        lock.lock();

                        try {
                            if (isDone())
                                return;

                            if (snapFut != null && (snapFut.isCancelled() || !snapFut.get()))
                                break;

                            snapFut = cctx.snapshotMgr().createRemoteSnapshot(nodeId, assigns);

                            if (log.isInfoEnabled())
                                log.info("Start partitions preloading [from=" + nodeId + "]");

                            if (log.isDebugEnabled())
                                log.debug("Current state: " + this);
                        }
                        finally {
                            lock.unlock();
                        }

                        snapFut.get();
                    }
                    catch (IgniteFutureCancelledCheckedException ignore) {
                        // No-op.
                    }
                    catch (IgniteCheckedException e) {
                        log.error(e.getMessage(), e);

                        onDone(e);
                    }
                }
            }
        });
    }

    /**
     * @param nodeId Node ID.
     * @param file Partition snapshot file.
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     */
    public void onPartitionSnapshotReceived(UUID nodeId, File file, int grpId, int partId) {
        if (log.isTraceEnabled()) {
            log.trace("Processing partition snapshot [grp=" + cctx.cache().cacheGroup(grpId) +
                ", p=" + partId + ", path=" + file + "]");
        }

        if (isDone())
            return;

        try {
            awaitCleanupIfNeeded(grpId);

            if (isDone())
                return;

            initialize(grpId, partId, file);

            cctx.filePreloader().changePartitionMode(grpId, partId, this::isDone).listen(f -> {
                try {
                    onPartitionSnapshotRestored(grpId, partId, f.get());
                } catch (IgniteCheckedException e) {
                    log.error("Unable to restore partition snapshot [grp=" + cctx.cache().cacheGroup(grpId) +
                        ", p=" + partId + "]");

                    onDone(e);
                }
            });

            int received = receivedCnt.incrementAndGet();

            if (received == partsToNodes.size()) {
                if (log.isDebugEnabled())
                    log.debug("All partition files received - triggering checkpoint to finish file rebalancing.");

                cctx.database().wakeupForCheckpoint("Checkpoint required to finish rebalancing.");
            }
        }
        catch (IOException | IgniteCheckedException e) {
            log.error("Unable to handle partition snapshot", e);

            onDone(e);
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntr The highest value of the update counter before this partition began to process updates.
     */
    private void onPartitionSnapshotRestored(int grpId, int partId, long cntr) {
        Integer partsCnt = remaining.get(grpId);

        assert partsCnt != null;

        Map<Integer, Long> cntrs = restored.computeIfAbsent(grpId, v-> new ConcurrentHashMap<>());

        cntrs.put(partId, cntr);

        if (partsCnt == cntrs.size())
            onCacheGroupDone(grpId, cntrs);
    }

    /**
     * @param grpId Group ID.
     * @param maxCntrs Partition set with HWM update counter value for hstorical rebalance.
     */
    private void onCacheGroupDone(int grpId, Map<Integer, Long> maxCntrs) {
        if (remaining.remove(grpId) == null)
            return;

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        assert !grp.localWalEnabled() : "grp=" + grp.cacheOrGroupName();

        Map<UUID, Map<Integer, T2<Long, Long>>> histAssignments = new HashMap<>();

        for (Map.Entry<Integer, Long> e : maxCntrs.entrySet()) {
            int partId = e.getKey();

            long initCntr = grp.topology().localPartition(partId).initialUpdateCounter();
            long maxCntr = e.getValue();

            assert maxCntr >= initCntr : "from=" + initCntr + ", to=" + maxCntr;

            if (initCntr != maxCntr) {
                long uniquePartId = ((long)grpId << 32) + partId;

                UUID nodeId = partsToNodes.get(uniquePartId);

                histAssignments.computeIfAbsent(nodeId, v -> new TreeMap<>()).put(partId, new T2<>(initCntr, maxCntr));

                continue;
            }

            if (log.isDebugEnabled())
                log.debug("No need for WAL rebalance [grp=" + grp.cacheOrGroupName() + ", p=" + partId + "]");
        }

        GridQueryProcessor qryProc = cctx.kernalContext().query();

        if (qryProc.moduleEnabled()) {
            if (log.isInfoEnabled())
                log.info("Starting index rebuild for cache group: " + grp.cacheOrGroupName());

            for (GridCacheContext ctx : grp.caches()) {
                IgniteInternalFuture<?> fut = qryProc.rebuildIndexesFromHash(ctx);

                if (fut != null)
                    idxRebuildFut.add(fut);
            }
        }
        else
        if (log.isInfoEnabled())
            log.info("Skipping index rebuild for cache group: " + grp.cacheOrGroupName());

        // Cache group file rebalancing is finished, historical rebalancing will send separate events
        grp.preloader().sendRebalanceFinishedEvent(exchId.discoveryEvent());

        int remain = remaining.size();

        log.info("Partition files preload complete [grp=" + grp.cacheOrGroupName() + ", remain=" + remain + "]");

        if (histAssignments.isEmpty())
            cctx.walState().onGroupRebalanceFinished(grp.groupId(), topVer);
        else
            requestHistoricalRebalance(grp, histAssignments);

        if (remain == 0) {
            idxRebuildFut.markInitialized();

            onDone(true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        boolean nodeIsStopping = X.hasCause(err, NodeStoppingException.class);

        if (cancel || err != null) {
            lock.lock();

            try {
                synchronized (this) {
                    if (isDone())
                        return true;

                    if (log.isInfoEnabled())
                        log.info("Cancelling file rebalancing.");

                    abortCb.run();

                    if (err == null) {
                        for (IgniteInternalFuture fut : offheapClearTasks.values()) {
                            if (!fut.isDone())
                                fut.get(MAX_MEM_CLEANUP_TIMEOUT);
                        }
                    }

                    if (snapFut != null && !snapFut.isDone()) {
                        if (log.isDebugEnabled())
                            log.debug("Cancelling snapshot creation [fut=" + snapFut + "]");

                        snapFut.cancel();
                    }

                    if (log.isDebugEnabled() && !idxRebuildFut.isDone() && !idxRebuildFut.futures().isEmpty()) {
                        log.debug("Index rebuild is still in progress, cancelling.");

                        idxRebuildFut.cancel();
                    }

                    for (Integer grpId : remaining.keySet()) {
                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        if (grp != null)
                            grp.preloader().sendRebalanceFinishedEvent(exchId.discoveryEvent());
                    }
                }
            }
            catch (IgniteCheckedException e) {
                if (err != null)
                    e.addSuppressed(err);

                log.error("Failed to cancel file rebalancing.", e);
            }
            finally {
                lock.unlock();
            }
        }

        return super.onDone(res, nodeIsStopping ? null : err, nodeIsStopping || cancel);
    }

    /**
     * @param grp Cache group.
     * @param assigns Assignments.
     */
    private void requestHistoricalRebalance(CacheGroupContext grp, Map<UUID, Map<Integer, T2<Long, Long>>> assigns) {
        GridDhtPreloaderAssignments grpAssigns = new GridDhtPreloaderAssignments(exchId, topVer);

        for (Map.Entry<UUID, Map<Integer, T2<Long, Long>>> entry : assigns.entrySet()) {
            ClusterNode node = cctx.discovery().node(entry.getKey());
            Map<Integer, T2<Long, Long>> nodeAssigns = entry.getValue();

            GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grp.groupId());

            for (Map.Entry<Integer, T2<Long, Long>> e : nodeAssigns.entrySet())
                msg.partitions().addHistorical(e.getKey(), e.getValue().get1(), e.getValue().get2(), nodeAssigns.size());

            grpAssigns.put(node, msg);
        }

        GridCompoundFuture<Boolean, Boolean> histFut = new GridCompoundFuture<>(CU.boolReducer());

        Runnable task = grp.preloader().addAssignments(grpAssigns, true, rebalanceId, null, histFut);

        cctx.kernalContext().getSystemExecutorService().submit(task);
    }

    /**
     * Re-initialize partition with a new file.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param src Partition snapshot file.
     * @throws IOException If was not able to move partition file.
     * @throws IgniteCheckedException If cache or partition with the given ID does not exists.
     */
    private void initialize(int grpId, int partId, File src) throws IOException, IgniteCheckedException {
        FilePageStore pageStore = ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId));

        File dest = new File(pageStore.getFileAbsolutePath());

        if (log.isDebugEnabled())
            log.debug("Moving partition file [from=" + src + " , to=" + dest + " , size=" + src.length() + "]");

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        assert !cctx.pageStore().exists(grpId, partId) :
            "Partition file exists [cache=" + grp.cacheOrGroupName() + ", p=" + partId + "]";

        Files.move(src.toPath(), dest.toPath());

        GridDhtLocalPartition part = grp.topology().localPartition(partId);

        part.dataStore().reinit();

        grp.preloader().rebalanceEvent(partId, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());
    }

    /**
     * Wait for region cleaning if necessary.
     *
     * @param grpId Cache group ID.
     * @throws IgniteCheckedException If the cleanup failed.
     */
    private void awaitCleanupIfNeeded(int grpId) throws IgniteCheckedException {
        try {
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            String region = grp.dataRegion().config().getName();

            IgniteInternalFuture clearTask = offheapClearTasks.get(region);

            if (clearTask.isCancelled()) {
                log.warning("Off-heap cleanup task has been cancelled [region=" + region + "]");

                return;
            }

            if (!clearTask.isDone() && log.isDebugEnabled())
                log.debug("Wait for region cleanup [grp=" + grp + "]");
            else if (clearTask.error() != null) {
                log.warning("Off heap region was not cleared properly [region=" + region + "]", clearTask.error());

                onDone(clearTask.error());

                return;
            }

            clearTask.get();
        } catch (IgniteFutureCancelledCheckedException ignore) {
            // No-op.
        }
    }

    // todo
    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("\n\treceived " + receivedCnt.get() + " out of " + partsToNodes.size());
        buf.append("\n\tRemainng: " + remaining);

        if (!snapFut.isDone())
            buf.append("\n\tSnapshot: " + snapFut.toString());

        buf.append("\n\tMemory regions:\n");

        for (Map.Entry<String, IgniteInternalFuture> entry : offheapClearTasks.entrySet())
            buf.append("\t\t" + entry.getKey() + " finished=" + entry.getValue().isDone() + ", failed=" + ((GridFutureAdapter)entry.getValue()).isFailed() + "\n");

        if (!isDone())
            buf.append("\n\tIndex future fnished=").append(idxRebuildFut.isDone()).append(" failed=").append(idxRebuildFut.isFailed()).append(" futs=").append(idxRebuildFut.futures()).append('\n');

        return buf.toString();
    }

    /** */
    private static class ClearRegionTask extends GridFutureAdapter {
        /** */
        private final Set<Long> parts;

        /** */
        private final DataRegion region;

        /** */
        private final GridCacheSharedContext cctx;

        /** */
        private final IgniteLogger log;

        /**
         * @param parts Parts.
         * @param region Region.
         * @param cctx Cache shared context.
         * @param log Logger.
         */
        public ClearRegionTask(Set<Long> parts, DataRegion region, GridCacheSharedContext cctx, IgniteLogger log) {
            this.parts = parts;
            this.region = region;
            this.cctx = cctx;
            this.log = log;
        }

        /**
         * Clears off-heap memory region.
         */
        public IgniteInternalFuture doClear() {
            PageMemoryEx memEx = (PageMemoryEx)region.pageMemory();

            if (log.isDebugEnabled())
                log.debug("Cleaning up region " + region.config().getName());

            memEx.clearAsync(
                (grp, pageId) -> parts.contains(((long)grp << 32) + PageIdUtils.partId(pageId)), true)
                .listen(c1 -> {
                    cctx.database().checkpointReadLock();

                    try {
                        if (log.isDebugEnabled())
                            log.debug("Off heap region cleared [region=" + region.config().getName() + "]");

                        invalidatePartitions(parts);

                        onDone();
                    }
                    catch (RuntimeException | IgniteCheckedException e) {
                        onDone(e);
                    }
                    finally {
                        cctx.database().checkpointReadUnlock();
                    }
                });

            return this;
        }

        /**
         * @param partSet Partition set.
         */
        private void invalidatePartitions(Set<Long> partSet) throws IgniteCheckedException {
            CacheGroupContext grp = null;
            int prevGrp = 0;

            for (long uniquePart : partSet) {
                int grpId = (int)(uniquePart >> 32);
                int partId = (int)uniquePart;

                if (prevGrp == 0|| prevGrp != grpId) {
                    grp = cctx.cache().cacheGroup(grpId);

                    prevGrp = grpId;
                }

                // Skip this group if it was stopped.
                if (grp == null)
                    continue;

                int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grpId, partId);

                ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId).truncate(tag);

                if (log.isDebugEnabled())
                    log.debug("Parition truncated [grp=" + grp.cacheOrGroupName() + ", p=" + partId + "]");
            }
        }
    }
}
