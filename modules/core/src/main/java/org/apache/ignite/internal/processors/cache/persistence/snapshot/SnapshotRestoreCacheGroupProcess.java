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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_FINISH;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreCacheGroupProcess {
    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Cache group restore operation was rejected. ";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore perform phase. */
    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> prepareRestoreProc;

    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> startCachesProc;

    private final DistributedProcess<SnapshotRestoreRollbackRequest, SnapshotRestoreRollbackResponse> rollbackRestoreProc;

    /** Logger. */
    private final IgniteLogger log;

    /** The future to be completed when the cache restore process is complete. */
    private volatile RestoreSnapshotFuture fut = new RestoreSnapshotFuture();

    /** Snapshot restore operation context. */
    private volatile SnapshotRestoreContext opCtx;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotRestoreCacheGroupProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

//        prepareRestoreProc =
//            new DistributedProcess<>(ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, this::prepare, this::finishPrepare);
        prepareRestoreProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, this::prepare, this::finishPrepare);

        startCachesProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_START, this::startCaches, this::finishStartCaches);

        rollbackRestoreProc = new DistributedProcess<>(
            ctx, RESTORE_CACHE_GROUP_SNAPSHOT_FINISH, this::complete, this::finishComplete);

        fut.onDone();
    }

    /**
     * Start cache group restore operation.
     *
     * @param snpName Snapshot name.
     * @param cacheGrpNames Name of the cache groups for restore.
     * @return Future that will be completed when the restore operation is complete and the cache groups are started.
     */
    public IgniteFuture<Void> start(String snpName, Collection<String> cacheGrpNames) {
        if (ctx.clientNode()) {
            return new IgniteFinishedFutureImpl<>(new UnsupportedOperationException("Client and daemon nodes can not " +
                "perform this operation."));
        }

        IgniteInternalFuture<Void> fut0 = fut;

        if (!fut0.isDone()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "The previous snapshot restore operation was not completed."));
        }

        DiscoveryDataClusterState clusterState = ctx.state().clusterState();

        if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        if (!clusterState.hasBaselineTopology()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "The baseline topology is not configured for cluster."));
        }

        if (ctx.cache().context().snapshotMgr().isSnapshotCreating()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG +
                "A cluster snapshot operation is in progress."));
        }

        Set<UUID> srvNodeIds = new HashSet<>(F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            F.node2id(), (node) -> CU.baselineNode(node, ctx.state().clusterState())));

        SnapshotInfoTask task = new SnapshotInfoTask();

        try {
            SnapshotRestoreCacheMeta meta =
                ctx.task().execute(task, new ValidateSnapshotTaskArg(snpName, cacheGrpNames)).get();

            // todo
            Set<String> foundGrps = new HashSet<>();

            for (CacheConfiguration<?,?> ccfg : F.viewReadOnly(meta.configs(), StoredCacheData::config)) {
                String grpName = ccfg.getGroupName() != null ? ccfg.getGroupName() : ccfg.getName();

                foundGrps.add(grpName);
            }

            if (!foundGrps.containsAll(cacheGrpNames)) {
                Set<String> missedGroups = new HashSet<>(cacheGrpNames);

                missedGroups.removeAll(foundGrps);

                throw new IllegalArgumentException("Cache group(s) not found in snapshot [groups=" +
                    F.concat(missedGroups, ", ") + ", snapshot=" + snpName + ']');
            }

            SnapshotRestoreRequest req = new SnapshotRestoreRequest(
                UUID.randomUUID(), snpName, cacheGrpNames, srvNodeIds, meta.configs(), meta.updateMetaNodeId);

            prepareRestoreProc.start(req.requestId(), req);

            return new IgniteFutureImpl<>(fut = new RestoreSnapshotFuture());
        }
        catch (IgniteCheckedException e) {
            return new IgniteFinishedFutureImpl<>(e);
        }
    }

    /**
     * Check if the cache group restore process is currently running.
     *
     * @return {@code True} if cache group restore process is currently running.
     */
    public boolean inProgress(@Nullable String cacheName) {
        RestoreSnapshotFuture fut0 = fut;

        return !staleFuture(fut0) && (cacheName == null || opCtx.containsCache(cacheName));
    }

    /**
     * @param fut The future of cache snapshot restore operation.
     * @return {@code True} if the future completed or not initiated.
     */
    public boolean staleFuture(RestoreSnapshotFuture fut) {
        return fut.isDone() || opCtx == null;
    }

    /**
     * @param cacheName Started cache name.
     * @param grpName Started cache group name.
     * @param err Error if any.
     */
    public void handleCacheStart(String cacheName, @Nullable String grpName, @Nullable Throwable err) {
        RestoreSnapshotFuture fut0 = fut;

        if (staleFuture(fut0))
            return;

//        opCtx.processCacheStart(cacheName, grpName, err, ctx.getSystemExecutorService(), fut0);
    }

    /**
     * Node left callback.
     *
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        RestoreSnapshotFuture fut0 = fut;

        if (staleFuture(fut0))
            return;

        if (opCtx.nodes().contains(leftNodeId)) {
            fut0.interrupt(new IgniteException(OP_REJECT_MSG +
                "Baseline node has left the cluster [nodeId=" + leftNodeId + ']'));
//            fut0.onDone(new IgniteException(OP_REJECT_MSG +
//                "Baseline node has left the cluster [nodeId=" + leftNodeId + ']'));
        }
    }

    /**
     * Abort the currently running restore procedure (if any).
     *
     * @param reason Interruption reason.
     */
    public void stop(String reason) {
        RestoreSnapshotFuture fut0 = fut;

        if (staleFuture(fut0))
            return;

        fut0.interrupt(new IgniteCheckedException("Restore process has been interrupted: " + reason));
    }

    /**
     * Ensures that a cache with the specified name does not exist locally.
     *
     * @param name Cache name.
     * @throws IllegalStateException If cache with the specified name already exists.
     */
    private void ensureCacheAbsent(String name) throws IllegalStateException {
        int id = CU.cacheId(name);

        if (ctx.cache().cacheGroupDescriptors().containsKey(id) || ctx.cache().cacheDescriptor(id) != null) {
            throw new IllegalStateException("Cache \"" + name +
                "\" should be destroyed manually before perform restore operation.");
        }
    }

    /**
     * @param req Request to perform snapshot restore.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestoreResponse> prepare(SnapshotRestoreRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        if (inProgress(null)) {
            return new GridFinishedFuture<>(
                new IgniteException(OP_REJECT_MSG + "The previous snapshot restore operation was not completed."));
        }

        if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
            return new GridFinishedFuture<>(new IllegalStateException(OP_REJECT_MSG + "The cluster should be active."));

        // Skip creating future on initiator.
        if (fut.isDone())
            fut = new RestoreSnapshotFuture();

        opCtx = new SnapshotRestoreContext(req.requestId(), req.snapshotName(), new HashSet<>(req.nodes()), req.groups(), ctx);

        fut.listen(f -> opCtx = null);

        RestoreSnapshotFuture fut0 = fut;

        SnapshotRestoreContext opCtx0 = opCtx;

        for (StoredCacheData cacheData : req.configs())
            opCtx0.addCacheData(cacheData);

        GridFutureAdapter<SnapshotRestoreResponse> retFut = new GridFutureAdapter<>();

        try {
            if (!ctx.cache().context().snapshotMgr().snapshotLocalDir(opCtx0.snapshotName()).exists())
                return new GridFinishedFuture<>();

            for (String grpName : opCtx0.groups())
                ensureCacheAbsent(grpName);

            for (StoredCacheData cfg : opCtx0.configs()) {
                if (!F.isEmpty(cfg.config().getGroupName()))
                    ensureCacheAbsent(cfg.config().getName());
            }

            ctx.cache().context().snapshotMgr().updateRestoredGroups(opCtx0.groups());

            boolean updateMeta = ctx.localNodeId().equals(req.updateMetaNodeId());

            ctx.getSystemExecutorService().submit(() -> {
                try {
                    opCtx0.restore(updateMeta, fut0::interrupted);

                    if (fut0.interrupted()) {
                        log.error("Snapshot restore process has been interrupted " +
                            "[groups=" + opCtx0.groups() + ", snapshot=" + opCtx0.snapshotName() + ']', fut0.error());

                        opCtx0.rollback();

                        ctx.cache().context().snapshotMgr().updateRestoredGroups(null);

                        retFut.onDone(fut0.error());
                    }
                    else
                        retFut.onDone();
                }
                catch (Throwable t) {
                    retFut.onDone(t);
                }
            });

            return retFut;
        } catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param reqId Request ID.
     * @param res Results.
     * @param errs Errors.
     */
    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestoreResponse> res, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || !reqId.equals(opCtx.requestId()))
            return;

        Exception failure = F.first(errs.values());

        if (failure != null) {
            opCtx.rollback();

            try {
                ctx.cache().context().snapshotMgr().updateRestoredGroups(null);
            }
            catch (IgniteCheckedException e) {
                log.error("Unable to reset cache group restore state", e);
            }

            fut0.onDone(failure);

            return;
        }

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return;

        // todo
        startCachesProc.start(reqId, new SnapshotRestoreRequest(reqId, null, null, null, null, null));
        //ctx.cache().dynamicStartCachesByStoredConf(opCtx.configs(), true, true, false, null, true);
    }

    /**
     * @param req Request to perform snapshot restore.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestoreResponse> startCaches(SnapshotRestoreRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || !req.requestId().equals(opCtx.requestId()))
            return new GridFinishedFuture<>();

        if (!req.requestId().equals(opCtx.requestId()))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        if (!ctx.state().clusterState().state().active())
            return new GridFinishedFuture<>(new IgniteCheckedException(OP_REJECT_MSG + "Cluster state has been changed."));

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return new GridFinishedFuture<>();

        GridFutureAdapter<SnapshotRestoreResponse> retFut = new GridFutureAdapter<>();

        log.info("Starting restored caches: " + F.viewReadOnly(opCtx.configs(), c -> c.config().getName()));

        ctx.cache().dynamicStartCachesByStoredConf(opCtx.configs(), true, true, true, null, true).listen(
            f -> {
                if (f.error() != null) {
                    log.error("Unable to start restored caches.", f.error());

                    retFut.onDone(f.error());
                }
//                else if (!Boolean.TRUE.equals(f.result()))
//                    retFut.onDone(new IgniteException("Unable to start required caches."));
                else
                    retFut.onDone();
            }
        );

        return retFut;
    }

    private void finishStartCaches(UUID reqId, Map<UUID, SnapshotRestoreResponse> res, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        // todo restartProxies should be called on all nodes
        if (fut0.isDone() || !reqId.equals(opCtx.requestId()))
            return;

        Exception failure = F.first(errs.values());

        if (failure == null && !res.keySet().containsAll(opCtx.nodes())) {
            Set<UUID> leftNodes = new HashSet<>(opCtx.nodes());

            leftNodes.removeAll(res.keySet());

            failure = new IgniteException(OP_REJECT_MSG + "Baseline node(s) has left the cluster [nodeId=" + leftNodes + ']');
        }

//        if (failure == null) {
//            System.out.println(Thread.currentThread().getName() + " >xxx> restartProxies");
//
//            ctx.cache().restartProxies();
//
//            fut0.onDone();
//
//            return;
//        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, new SnapshotRestoreRollbackRequest(reqId, failure == null, failure));
    }

    /**
     * @param req Request to perform snapshot restore.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestoreRollbackResponse> complete(SnapshotRestoreRollbackRequest req) {
        System.out.println(">xxx> start rollback [0]");

        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        RestoreSnapshotFuture fut0 = fut;

        // todo restartproxies should call on all nodes
        if (fut0.isDone() || !req.requestId().equals(opCtx.requestId()))
            return new GridFinishedFuture<>();

        if (!req.requestId().equals(opCtx.requestId()))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        if (req.activateCaches()) {
            System.out.println(Thread.currentThread().getName() + " >xxx> restartProxies");

            try {
                ctx.cache().restartProxies();
            } catch (Exception e) {
                return new GridFinishedFuture<>(e);
            }

            return new GridFinishedFuture<>();
        }

        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return new GridFinishedFuture<>();

        System.out.println(">xxx> start rollback");

        assert req.failure() != null;

        // todo should interrupt all operations on deactivation  -check "interrupted" flag and done future
        if (ctx.state().clusterState().state() != ClusterState.ACTIVE) {
            Throwable fail = req.failure();

            fail.addSuppressed(new IgniteCheckedException("Rollback operation suspended, cluster state has been changed."));

            return new GridFinishedFuture<>(new SnapshotRestoreRollbackResponse(fail, false));
        }

        GridFutureAdapter<SnapshotRestoreRollbackResponse> retFut = new GridFutureAdapter<>();

        List<String> cacheNames = new ArrayList<>(F.viewReadOnly(opCtx.configs(), data -> data.config().getName()));

        try {
            System.out.println("destroy caches...");

            ctx.cache().dynamicDestroyCaches(cacheNames, false, true).listen(
                f -> {
                    System.out.println("finished destroying  caches...");

                    Throwable procFailure = req.failure();
                    Throwable destroyErr = f.error();

                    if (destroyErr != null) {
                        log.error("Unable to destroy caches during snapshot restore procedure.", destroyErr);

                        procFailure.addSuppressed(destroyErr);
                    }

                    retFut.onDone(new SnapshotRestoreRollbackResponse(procFailure, destroyErr == null));
                }
            );
        } catch (Exception e) {
            log.error("Unable to destroy caches during snapshot restore procedure.", e);

            Throwable err0 = req.failure();

            err0.addSuppressed(e);

            return new GridFinishedFuture<>(new SnapshotRestoreRollbackResponse(err0, false));
        }

        return retFut;
    }

    private void finishComplete(UUID reqId, Map<UUID, SnapshotRestoreRollbackResponse> res, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        // todo restartProxies should be called on all nodes
        if (fut0.isDone() || !reqId.equals(opCtx.requestId()))
            return;

        Exception failure = F.first(errs.values());

        if (failure == null) {
            SnapshotRestoreRollbackResponse resp = F.first(F.viewReadOnly(res.values(), v -> v, v -> v != null));

            if (resp == null || resp.completed()) {
                try {
                    ctx.cache().context().snapshotMgr().updateRestoredGroups(null);
                }
                catch (IgniteCheckedException e) {
                    log.error("Unable to reset restored groups", e);
                }
            }

            if (resp != null)
                fut0.onDone(resp.failure());
            else
                fut0.onDone();

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            rollbackRestoreProc.start(reqId, new SnapshotRestoreRollbackRequest(reqId, false, failure));

//        if (failure == null && !res.keySet().containsAll(opCtx.nodes())) {
//            Set<UUID> leftNodes = new HashSet<>(opCtx.nodes());
//
//            leftNodes.removeAll(res.keySet());
//
//            failure = new IgniteException(OP_REJECT_MSG + "Baseline node(s) has left the cluster [nodeId=" + leftNodes + ']');
//        }

//        // todo should store global failure in case coordinator fails
//        assert failure != null;
//
//        fut0.onDone(failure);
    }

    public void cleanup(Collection<String> grps) {
        for (String grpName : grps) {
            CacheGroupDescriptor desc = ctx.cache().cacheGroupDescriptor(CU.cacheId(grpName));

            // todo snapshot folder celanup folder
            File cacheDir = ctx.cache().context().snapshotMgr().resolveSnapshotCacheDir("testSnapshot", grpName);
            log.info("Cleaning up " + cacheDir);
            U.delete(cacheDir);

            if (desc == null)
                continue;

            ctx.cache().context().affinity().stopCacheGroupOnReconnect(CU.cacheId(grpName));

            CacheGroupDescriptor obj = ctx.cache().cacheGroupDescriptors().remove(CU.cacheId(grpName));

            assert obj != null : grpName;

//            grp =
//            CacheGroupContext grp = ctx.cache().cacheGroup(CU.cacheId(grpName));
//
//            if (grp != null) {
//                ctx.cache().context().database().checkpointReadLock();
//
//                try {
//                    for (GridCacheContext<?, ?> cacheCtx : grp.caches())
//                        ctx.cache().prepareCacheStop(cacheCtx.name(), false);
//                }
//                finally {
//                    ctx.cache().context().database().checkpointReadUnlock();
//                }
//
//                ctx.cache().stopCacheGroup(grp.groupId(), false);
//
//                ctx.cache().context().database().onCacheGroupsStopped(Collections.singleton(new T2<>(grp, false)));
//            }
//            else
//                assert false : grpName;

            //ctx.cache().prepareCacheStop(grpName, true);

//            desc = ctx.cache().cacheGroupDescriptor(CU.cacheId(grpName));

//            assert desc == null : grpName;
        }
    }

    private static class SnapshotRestoreCacheMeta implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        private List<StoredCacheData> ccfgs;

        private Map<String, Set<Integer>> partIds;

        private UUID updateMetaNodeId;

        public SnapshotRestoreCacheMeta(List<StoredCacheData> ccfgs, Map<String, Set<Integer>> partIds, @Nullable UUID updateMetaNodeId) {
            this.ccfgs = ccfgs;
            this.partIds = partIds;
            this.updateMetaNodeId = updateMetaNodeId;
        }

        public List<StoredCacheData> configs() {
            return ccfgs;
        }

        public Map<String, Set<Integer>> partIds() {
            return partIds;
        }

        public UUID updateMetaNodeId() {
            return updateMetaNodeId;
        }
    }

    private static class SnapshotInfoTask extends ComputeTaskAdapter<ValidateSnapshotTaskArg, SnapshotRestoreCacheMeta> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, ValidateSnapshotTaskArg arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid)
                jobs.put(new SnapshotInfoJob(arg), node);

            return jobs;

        }

        @Nullable @Override public SnapshotRestoreCacheGroupProcess.SnapshotRestoreCacheMeta reduce(List<ComputeJobResult> results) throws IgniteException {
            Map<String, Set<Integer>> grpPartIds = new HashMap<>();
            List<StoredCacheData> ccfgs = Collections.emptyList();
            UUID firstSnapshotDataNode = null;

            for (ComputeJobResult jobRes : results) {
                SnapshotRestoreCacheMeta res = jobRes.getData();

                if (res == null)
                    continue;

                if (firstSnapshotDataNode == null) {
                    firstSnapshotDataNode = jobRes.getNode().id();
                    ccfgs = res.configs();
                }
                else if (res.configs().size() != res.configs().size()) {
                    throw new IgniteException("Count of cache configs in shared group mismatch [" +
                        "node1=" + firstSnapshotDataNode + ", cnt=" + ccfgs.size() +
                        ", node2=" + jobRes.getNode().id() + ", cnt=" + res.configs().size() + ']');
                }

                for (Map.Entry<String, Set<Integer>> e : res.partIds().entrySet())
                    grpPartIds.computeIfAbsent(e.getKey(), v -> new HashSet<>()).addAll(e.getValue());
            }

            for (StoredCacheData cacheData : ccfgs) {
                CacheConfiguration<?, ?> ccfg = cacheData.config();

                String grpName = ccfg.getGroupName() != null ? ccfg.getGroupName() : ccfg.getName();

                Set<Integer> partIds = grpPartIds.get(grpName);

                int reqParts = ccfg.getAffinity().partitions();
                int availParts = partIds.size();

                if (reqParts != availParts) {
                    throw new IgniteException("Cannot restore snapshot, not all partitions available [" +
                        "required=" + reqParts +
                        ", avail=" + availParts +
                        ", group=" + grpName + ']');
                }
            }

            return new SnapshotRestoreCacheMeta(ccfgs, grpPartIds, firstSnapshotDataNode);
        }
    }

    /** */
    private static class SnapshotInfoJob extends ComputeJobAdapter {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Auto-injected grid instance.
         */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        private final ValidateSnapshotTaskArg req;

        public SnapshotInfoJob(ValidateSnapshotTaskArg req) {
            this.req = req;
        }

        @Override public Object execute() throws IgniteException {
            try {
                return execute0();
            }
            catch (BinaryObjectException e) {
                //log.warning(OP_REJECT_MSG + "Incompatible binary types found", e);
                throw new IgniteException(OP_REJECT_MSG + "Incompatible binary types found: " + e.getMessage());

            } catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        private SnapshotRestoreCacheMeta execute0() throws IgniteCheckedException {
            IgniteSnapshotManager snapshotMgr = ignite.context().cache().context().snapshotMgr();

            Map<String, StoredCacheData> cacheCfgs = new HashMap<>();
            Map<String, Set<Integer>> partIds = new HashMap<>();

            // Collect cache configuration(s).
            for (String grpName : req.groups()) {
                File cacheDir = snapshotMgr.resolveSnapshotCacheDir(req.snapshotName(), grpName);

                if (!cacheDir.exists())
                    return null;

                FilePageStoreManager pageStoreMgr = (FilePageStoreManager)ignite.context().cache().context().pageStore();

                pageStoreMgr.readCacheConfigurations(cacheDir, cacheCfgs);

                partIds.put(grpName, pageStoreMgr.scanPartitionIds(cacheDir));
            }

            if (cacheCfgs.isEmpty())
                return null;

            File binDir = binaryWorkDir(snapshotMgr.snapshotLocalDir(req.snapshotName()).getAbsolutePath(),
                ignite.context().pdsFolderResolver().resolveFolders().folderName());

            ignite.context().cacheObjects().checkMetadata(binDir);

            return new SnapshotRestoreCacheMeta(new ArrayList<>(cacheCfgs.values()), partIds, null);
        }
    }

    /** */
    private class RestoreSnapshotFuture extends GridFutureAdapter<Void> {
        /** The exception that led to the interruption of the process. */
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();

        /**
         * @return Interrupted flag.
         */
        public boolean interrupted() {
            return errRef.get() != null;
        }

        public boolean interrupt(Throwable t) {
            return errRef.compareAndSet(null, t);
        }

//        /** {@inheritDoc} */
//        @Override protected boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
//            if (err == null)
//                return super.onDone(res, err, cancel);
//
//            if (errRef.compareAndSet(null, err)) {
//                SnapshotRestoreContext opCtx0 = opCtx;
//
//                Set<String> grpNames = opCtx0.groups();
//
//                log.error("Snapshot restore process has been interrupted " +
//                    "[groups=" + grpNames + ", snapshot=" + opCtx0.snapshotName() + ']', err);
//
//                for (String grpName : grpNames)
//                    opCtx0.rollback(grpName);
//
//                return super.onDone(res, err, cancel);
//            }
//
//            return false;
//        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RestoreSnapshotFuture.class, this);
        }
    }
}
