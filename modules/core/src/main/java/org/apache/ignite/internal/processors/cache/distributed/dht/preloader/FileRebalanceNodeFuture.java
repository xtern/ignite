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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** */
public class FileRebalanceNodeFuture extends GridFutureAdapter<Boolean> {
    /** Context. */
    protected GridCacheSharedContext cctx;

    /** Logger. */
    private final IgniteLogger log;

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
    private Map<Integer, Set<PartCounters>> remainingHist;

    /** {@code True} if the initial demand request has been sent. */
    private AtomicBoolean initReq = new AtomicBoolean();

    /** */
    private final ClusterNode node;

    /** */
    private final FileRebalanceFuture mainFut;

    /** Cache group rebalance order. */
    private final int rebalanceOrder;

    /** Node snapshot name. */
    private volatile IgniteInternalFuture<Boolean> snapFut;

    /** */
//    public IgniteInternalFuture<Boolean> snapshotFuture() {
//        return snapFut;
//    }

    /**
     * Default constructor for the dummy future.
     */
    public FileRebalanceNodeFuture() {
        this(null, null, null, null, 0, 0, Collections.emptyMap(), null);

        onDone();
    }

    /**
     * @param node Supplier node.
     * @param rebalanceId Rebalance id.
     * @param assigns Map of assignments to request from remote.
     * @param topVer Topology version.
     */
    public FileRebalanceNodeFuture(
        GridCacheSharedContext cctx,
        FileRebalanceFuture mainFut,
        IgniteLogger log,
        ClusterNode node,
        int rebalanceOrder,
        long rebalanceId,
        Map<Integer, Set<Integer>> assigns,
        AffinityTopologyVersion topVer
    ) {
        this.cctx = cctx;
        this.mainFut = mainFut;
        this.log = log;
        this.node = node;
        this.rebalanceOrder = rebalanceOrder;
        this.rebalanceId = rebalanceId;
        this.assigns = assigns;
        this.topVer = topVer;

        remaining = new ConcurrentHashMap<>(assigns.size());
        remainingHist = new ConcurrentHashMap<>(assigns.size());

        for (Map.Entry<Integer, Set<Integer>> entry : assigns.entrySet()) {
            Set<Integer> parts = entry.getValue();
            int grpId = entry.getKey();

            assert !remaining.containsKey(grpId);

            remaining.put(grpId, new GridConcurrentHashSet<>(entry.getValue()));
        }
    }

    /**
     * @return Rebalancing order.
     */
    public int order() {
        return rebalanceOrder;
    }

    /**
     * @return Supplier node ID.
     */
    public UUID nodeId() {
        return node.id();
    }

    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /**
     * @param grpId Cache group id to search.
     * @param partId Cache partition to remove;
     */
    public void onPartitionRestored(int grpId, int partId, long min, long max) {
        Set<Integer> parts = remaining.get(grpId);

        assert parts != null : "Unexpected group identifier: " + grpId;

        remainingHist.computeIfAbsent(grpId, v -> new ConcurrentSkipListSet<>())
            .add(new PartCounters(partId, min, max));

        if (log.isDebugEnabled()) {
            log.debug("Partition done [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() +
                ", p=" + partId + ", remaining=" + parts.size() + "]");
        }

        boolean rmvd = parts.remove(partId);

        assert rmvd : "Partition not found: " + partId;

        if (parts.isEmpty())
            onGroupRestored(grpId);
    }

    private void onGroupRestored(int grpId) {
        Set<Integer> parts = remaining.remove(grpId);

        if (parts == null)
            return;

        Set<PartCounters> histParts = remainingHist.remove(grpId);

        assert histParts.size() == assigns.get(grpId).size() : "expect=" + assigns.get(grpId).size() + ", actual=" + histParts.size();

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grpId);

        for (PartCounters desc : histParts) {
            assert desc.toCntr >= desc.fromCntr : "from=" + desc.fromCntr + ", to=" + desc.toCntr;

            if (desc.fromCntr != desc.toCntr) {
                if (log.isDebugEnabled()) {
                    log.debug("Prepare to request historical rebalancing [cache=" + grp.cacheOrGroupName() + ", p=" +
                        desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");
                }

                // todo histParts.size incorrect
                msg.partitions().addHistorical(desc.partId, desc.fromCntr, desc.toCntr, histParts.size());

                continue;
            }

            log.debug("Skipping historical rebalancing [p=" +
                desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");

            // No historical rebalancing required  -can own partition.
            if (grp.localWalEnabled()) {
                boolean owned = grp.topology().own(grp.topology().localPartition(desc.partId));

                assert owned : "part=" + desc.partId + ", grp=" + grp.cacheOrGroupName();
            }
        }

        mainFut.onCacheGroupDone(grpId, nodeId(), msg);

        if (remaining.isEmpty() && !isDone())
            onDone(true);

//        if (!msg.partitions().hasHistorical()) {
//            mainFut.onCacheGroupDone(grpId, nodeId(), false);
//

//
//            return;
//        }
//
//        GridDhtPartitionExchangeId exchId = cctx.exchange().lastFinishedFuture().exchangeId();
//
//        GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchId, topVer);
//
//        assigns.put(node, msg);
//
//        GridCompoundFuture<Boolean, Boolean> histFut = new GridCompoundFuture<>(CU.boolReducer());
//
//        Runnable task = grp.preloader().addAssignments(assigns, true, rebalanceId, null, histFut);
//
//        if (log.isDebugEnabled())
//            log.debug("Starting historical rebalancing [node=" + node.id() + ", cache=" + grp.cacheOrGroupName() + "]");
//
//        task.run();
//
//        histFut.markInitialized();
//
//        histFut.listen(c -> {
//            try {
//                if (isDone())
//                    return;
//
//                mainFut.onCacheGroupDone(grpId, nodeId(), true);
//
//                // todo Test cancel of historical rebalancing + redundant forceFut.get() it's called onDone(cancelled)
//                if (histFut.isCancelled() && !histFut.get()) {
//                    log.warning("Cancelling file rebalancing due to unsuccessful historical rebalance [cancelled=" +
//                        histFut.isCancelled() + ", failed=" + histFut.isFailed() + "]");
//
//                    cancel();
//
//                    return;
//                }
//
//                if (remaining.isEmpty())
//                    onDone(true);
//            }
//            catch (IgniteCheckedException e) {
//                onDone(e);
//            }
//        });
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        if (isDone())
            return false;

        boolean r = super.onDone(res, err, cancel);

        try {
            if (snapFut != null && !snapFut.isDone())
                snapFut.cancel();
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to finish file rebalancing node routine", e);
        }

        mainFut.onNodeDone(this, res, err, cancel);

        return r;
    }

    /**
     * Request a remote snapshot of partitions.
     */
    public void requestPartitions() {
        try {
            snapFut = cctx.snapshotMgr().createRemoteSnapshot(node.id(), assigns);

            if (log.isInfoEnabled())
                log.info("Start partitions preloading [from=" + node.id() + ", snapshot=" + snapFut + ", fut=" + this + ']');
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to create remote snapshot [from=" + node.id() + ", assigns=" + assigns + "]", e);

            onDone(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "finished=" + isDone() + ", node=" + node.id() + ", grps=" + F.transform(assigns.keySet(), v -> cctx.cache().cacheGroup(v).cacheOrGroupName());
    }

    private static class PartCounters implements Comparable {
        /** Partition id. */
        final int partId;

        /** From counter. */
        final long fromCntr;

        /** To counter. */
        final long toCntr;

        public PartCounters(int partId, long fromCntr, long toCntr) {
            this.partId = partId;
            this.fromCntr = fromCntr;
            this.toCntr = toCntr;
        }

        @Override public int compareTo(@NotNull Object o) {
            PartCounters otherDesc = (PartCounters)o;

            if (partId > otherDesc.partId)
                return 1;

            if (partId < otherDesc.partId)
                return -1;

            return 0;
        }
    }
}

