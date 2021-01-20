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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestorePrepareResponse.CacheGroupSnapshotDetails;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT_RESTORE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT_RESTORE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.UNDO_SNAPSHOT_RESTORE;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreCacheGroupProcess {
    /** Reject operation message. */
    private static final String OP_REJECT_MSG = "Snapshot restore operation was rejected. ";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache group restore prepare phase. */
    private final DistributedProcess<SnapshotRestorePrepareRequest, SnapshotRestorePrepareResponse> prepareRestoreProc;

    /** Cache group restore perform phase. */
    private final DistributedProcess<SnapshotRestorePerformRequest, SnapshotRestorePerformResponse> performRestoreProc;

    /** Cache group restore rollback phase. */
    private final DistributedProcess<SnapshotRestoreRollbackRequest, SnapshotRestoreRollbackResponse> rollbackRestoreProc;

    /** Logger. */
    private final IgniteLogger log;

    /** Restore operation lock. */
    private final ReentrantLock rollbackLock = new ReentrantLock();

    /** The future to be completed when the cache restore process is complete. */
    private volatile RestoreSnapshotFuture fut = new RestoreSnapshotFuture(false);

    /** Stopped flag. */
    private volatile boolean stopped;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotRestoreCacheGroupProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        prepareRestoreProc = new DistributedProcess<>(ctx, START_SNAPSHOT_RESTORE, this::prepare, this::finishPrepare);
        performRestoreProc = new DistributedProcess<>(ctx, END_SNAPSHOT_RESTORE, this::perform, this::finishPerform);
        rollbackRestoreProc = new DistributedProcess<>(ctx, UNDO_SNAPSHOT_RESTORE, this::rollback, this::finishRollback);

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

        if (!ctx.state().clusterState().state().active())
            return new IgniteFinishedFutureImpl<>(new IgniteException(OP_REJECT_MSG + "The cluster should be active."));

        Set<UUID> srvNodeIds = new HashSet<>(F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            F.node2id(),
            (node) -> CU.baselineNode(node, ctx.state().clusterState())));

        SnapshotRestorePrepareRequest req = new SnapshotRestorePrepareRequest(UUID.randomUUID(), snpName, cacheGrpNames, srvNodeIds);

        fut = new RestoreSnapshotFuture(true);

        prepareRestoreProc.start(req.requestId(), req);

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Check if the cache group restore process is currently running.
     *
     * @return {@code True} if cache group restore process is currently running.
     */
    public boolean inProgress(@Nullable String cacheName) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || fut0.request() == null)
            return false;

        return cacheName == null || fut0.containsCache(cacheName);
    }

    /**
     * @param cacheName Started cache name.
     * @param grpName Started cache group name.
     * @param err Error if any.
     */
    public void handleCacheStart(String cacheName, @Nullable String grpName, @Nullable Throwable err) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone() || fut0.request() == null)
            return;

        String grpName0 = grpName != null ? grpName : cacheName;

        PendingStartCacheGroup pendingGrp = fut0.pendingStartCaches.get(grpName0);

        // If any of shared caches has been started - we cannot rollback changes.
        if (pendingGrp.caches.remove(cacheName) && err == null)
            pendingGrp.canRollback = false;

        if (!pendingGrp.caches.isEmpty())
            return;

        if (pendingGrp.canRollback && err != null && fut.rollbackContext() != null) {
            ctx.getSystemExecutorService().submit(() -> {
                rollbackChanges(fut0, grpName0);

                fut0.onDone(err);
            });

            return;
        }

        fut0.pendingStartCaches.remove(grpName0);

        if (fut0.pendingStartCaches.isEmpty())
            fut0.onDone();
    }

    /**
     * Rollback changes made by process.
     *
     * @param fut Restore future.
     * @param grpName Cache group name.
     * @return {@code True} if changes were rolled back, {@code False} if changes have been already rolled back.
     */
    public boolean rollbackChanges(RestoreSnapshotFuture fut, String grpName) {
        rollbackLock.lock();

        try {
            if (fut.isDone())
                return false;

            List<File> createdFiles = fut.rollbackContext().remove(grpName);

            if (F.isEmpty(createdFiles))
                return false;

            ctx.cache().context().snapshotMgr().rollbackRestoreOperation(createdFiles);
        } finally {
            rollbackLock.unlock();
        }

        return true;
    }

    /**
     * Node left callback.
     *
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone())
            return;

        SnapshotRestorePrepareRequest req = fut0.request();

        if (req != null && req.requiredNodes().contains(leftNodeId)) {
            fut.handleError(new IgniteException(OP_REJECT_MSG +
                "Baseline node has left the cluster [nodeId=" + leftNodeId + ']'));
        }
    }

    /**
     * Abort the currently running restore procedure (if any).
     *
     * @param reason Interruption reason.
     */
    public void stop(String reason) {
        stopped = true;

        if (ctx.clientNode())
            return;

        RestoreSnapshotFuture fut0 = fut;

        if (fut0.isDone())
            return;

        SnapshotRestorePrepareRequest req = fut0.request();

        if (req == null)
            return;

        log.warning("Snapshot restore process has been interrupted [grps=" + req.groups() + ']');

        for (String grpName : fut0.request().groups())
            rollbackChanges(fut0, grpName);

        fut0.onDone(new IgniteCheckedException("Restore process has been interrupted: " + reason));
    }

    /** */
    public void start() {
        stopped = false;
    }

    /**
     * @param req Snapshot restore request.
     * @return Result future.
     */
    private IgniteInternalFuture<SnapshotRestorePrepareResponse> prepare(SnapshotRestorePrepareRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        if (inProgress(null))
            return errResponse(OP_REJECT_MSG + "The previous snapshot restore operation was not completed.");

        if (!ctx.state().clusterState().state().active())
            return errResponse(new IllegalStateException(OP_REJECT_MSG + "The cluster should be active."));

        if (fut.isDone())
            fut = new RestoreSnapshotFuture(false);

        fut.request(req);

        List<CacheGroupSnapshotDetails> grpCfgs = new ArrayList<>();

        // Collect cache configuration(s).
        for (String cacheName : req.groups()) {
            try {
                CacheGroupSnapshotDetails grpCfg = readCacheGroupDetails(req.snapshotName(), cacheName);

                if (grpCfg != null)
                    grpCfgs.add(grpCfg);
            }
            catch (IOException | IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }

        if (grpCfgs.isEmpty())
            return new GridFinishedFuture<>(new SnapshotRestorePrepareResponse(grpCfgs));

        try {
            ctx.cache().context().snapshotMgr().checkMetaCompatibility(req.snapshotName());
        }
        catch (BinaryObjectException e) {
            log.warning(OP_REJECT_MSG + "Incompatible binary types found", e);

            return errResponse(OP_REJECT_MSG + "Incompatible binary types found: " + e.getMessage());
        }
        catch (IOException | IgniteCheckedException e) {
            return errResponse(new IgniteException("Prepare phase has failed: " + e.getMessage(), e));
        }

        return new GridFinishedFuture<>(new SnapshotRestorePrepareResponse(grpCfgs));
    }

    /**
     * @param snapshotName Snapshot name.
     * @param grpName Cache group name.
     * @return Details about the locally stored cache group, or {@code null} if cache group (or snapshot) was not found.
     * @throws IgniteCheckedException if failed.
     * @throws IOException if I/O errors occur during reading cache configurations.
     */
    private @Nullable CacheGroupSnapshotDetails readCacheGroupDetails(String snapshotName, String grpName) throws IgniteCheckedException, IOException {
        File cacheDir = ctx.cache().context().snapshotMgr().resolveSnapshotCacheDir(snapshotName, ctx.config(), grpName);

        if (!cacheDir.exists())
            return null;

        Set<Integer> parts = new HashSet<>();

        List<StoredCacheData> cacheCfgs = new ArrayList<>(1);

        for (File file : cacheDir.listFiles()) {
            if (file.isDirectory())
                continue;

            String name = file.getName();

            if (name.endsWith(CACHE_DATA_FILENAME) && file.length() > 0)
                cacheCfgs.add(unmarshal(ctx.config(), file));
            else if (name.startsWith(FilePageStoreManager.PART_FILE_PREFIX)) {
                String partId = name.substring(FilePageStoreManager.PART_FILE_PREFIX.length(), name.indexOf('.'));

                parts.add(Integer.parseInt(partId));
            }
        }

        boolean sharedGrp = cacheDir.getName().startsWith(CACHE_GRP_DIR_PREFIX);

        return new CacheGroupSnapshotDetails(grpName, sharedGrp, cacheCfgs, parts);
    }

    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestorePrepareResponse> res, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        if (!errs.isEmpty()) {
            completeFuture(reqId, errs, fut0);

            return;
        }

        if (fut0.failure() != null) {
            fut0.onDone(fut0.failure());

            return;
        }

        UUID updateMetadataNode = null;

        for (Map.Entry<UUID, SnapshotRestorePrepareResponse> entry : res.entrySet()) {
            SnapshotRestorePrepareResponse resp = entry.getValue();

            if (!F.isEmpty(resp.groups())) {
                updateMetadataNode = entry.getKey();

                break;
            }
        }

        List<String> notFoundGroups = new ArrayList<>(fut0.request().groups());

        try {
            Collection<CacheGroupSnapshotDetails> grpsDetails = mergeNodeResults(res);

            List<StoredCacheData> cacheCfgs = new ArrayList<>();

            for (CacheGroupSnapshotDetails grpDetails : grpsDetails) {
                StoredCacheData cdata = F.first(grpDetails.configs());

                if (cdata == null)
                    continue;

                int reqParts = cdata.config().getAffinity().partitions();
                int availParts = grpDetails.parts().size();

                if (reqParts != availParts) {
                    throw new IgniteCheckedException("Cannot restore snapshot, not all partitions available [" +
                        "required=" + reqParts + ", avail=" + availParts + ", grp=" + grpDetails.groupName() + ']');
                }

                notFoundGroups.remove(grpDetails.groupName());

                PendingStartCacheGroup pendingGrp = fut0.pendingStartCaches.get(grpDetails.groupName());

                for (StoredCacheData cacheData : grpDetails.configs()) {
                    String cacheName = cacheData.config().getName();

                    // todo replace shared with groupName
                    if (grpDetails.shared()) {
                        fut0.addCacheId(CU.cacheId(cacheName));
                        pendingGrp.caches.add(cacheName);
                    }

                    cacheCfgs.add(cacheData);

                    CacheGroupDescriptor desc = ctx.cache().cacheGroupDescriptor(CU.cacheId(cacheName));

                    if (desc != null) {
                        throw new IllegalStateException("Cache \"" + desc.cacheOrGroupName() +
                            "\" should be destroyed manually before perform restore operation.");
                    }
                }
            }

            if (!notFoundGroups.isEmpty()) {
                throw new IllegalArgumentException("Cache group(s) \"" + F.concat(notFoundGroups, ", ") +
                    "\" not found in snapshot \"" + fut0.request().snapshotName() + "\"");
            }

            Set<UUID> srvNodeIds = new HashSet<>(F.viewReadOnly(ctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
                F.node2id(),
                (node) -> CU.baselineNode(node, ctx.state().clusterState())));

            Set<UUID> reqNodes = new HashSet<>(fut0.request().requiredNodes());

            reqNodes.removeAll(srvNodeIds);

            if (!reqNodes.isEmpty()) {
                throw new IllegalStateException("Unable to perform a restore operation, server node(s) left " +
                    "the cluster [nodeIds=" + F.concat(reqNodes, ", ") + ']');
            }

            fut0.startConfigs(cacheCfgs);
        }
        catch (Exception e) {
            fut0.onDone(e);

            return;
        }

        SnapshotRestorePrepareRequest req = fut0.request();

        if (U.isLocalNodeCoordinator(ctx.discovery()) && !fut0.isDone())
            performRestoreProc.start(reqId,
                new SnapshotRestorePerformRequest(
                    req.requestId(),
                    req.snapshotName(),
                    req.groups(),
                    req.requiredNodes(),
                    updateMetadataNode)
            );
    }

    private Collection<CacheGroupSnapshotDetails> mergeNodeResults(Map<UUID, SnapshotRestorePrepareResponse> responses) {
        Map<String, T2<UUID, CacheGroupSnapshotDetails>> globalDetails = new HashMap<>();

        for (Map.Entry<UUID, SnapshotRestorePrepareResponse> entry : responses.entrySet()) {
            UUID currNodeId = entry.getKey();
            SnapshotRestorePrepareResponse singleResp = entry.getValue();

            for (CacheGroupSnapshotDetails nodeDetails : singleResp.groups()) {
                T2<UUID, CacheGroupSnapshotDetails> clusterDetailsPair = globalDetails.get(nodeDetails.groupName());

                String grpName = nodeDetails.groupName();

                if (clusterDetailsPair == null) {
                    globalDetails.put(grpName, new T2<>(currNodeId, nodeDetails));

                    continue;
                }

                CacheGroupSnapshotDetails clusterDetails = clusterDetailsPair.get2();

                int currCfgCnt = nodeDetails.configs().size();
                int savedCfgCnt = clusterDetails.configs().size();

                if (currCfgCnt != savedCfgCnt) {
                    throw new IllegalStateException("Count of cache configs in shared group mismatch [" +
                        "node1=" + clusterDetailsPair.get1() + ", cnt=" + savedCfgCnt +
                        ", node2=" + currNodeId + ", cnt=" + nodeDetails.configs().size() + ']');
                }

                clusterDetails.parts().addAll(nodeDetails.parts());
            }
        }

        return F.viewReadOnly(globalDetails.values(), IgniteBiTuple::get2);
    }

    private IgniteInternalFuture<SnapshotRestorePerformResponse> perform(SnapshotRestorePerformRequest req) {
        if (ctx.clientNode() || !req.requiredNodes().contains(ctx.localNodeId()))
            return new GridFinishedFuture<>();

        RestoreSnapshotFuture fut0 = fut;

        SnapshotRestorePrepareRequest req0 = fut0.request();

        if (req0 == null || !req.requestId().equals(req0.requestId()))
            return errResponse("Unknown snapshot restore operation was rejected.");

        GridFutureAdapter<SnapshotRestorePerformResponse> retFut = new GridFutureAdapter<>();

        // todo check that snapshot exists
        ctx.getSystemExecutorService().submit(() -> {
            try {
                performRestore(req, fut0.rollbackContext());

                retFut.onDone();
            } catch (Throwable t) {
                retFut.onDone(t);
            }
        });

        return retFut;
    }

    private void performRestore(SnapshotRestorePerformRequest req, RestoreOperationContext opCtx) throws IgniteCheckedException {
        IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();

        if (ctx.localNodeId().equals(req.updateMetaNodeId()) && !stopped)
            snapshotMgr.registerSnapshotMetadata(req.snapshotName());

        for (String grpName : req.groups()) {
            rollbackLock.lock();

            try {
                if (stopped)
                    return;

                List<File> newFiles = new ArrayList<>();

                opCtx.put(grpName, newFiles);

                snapshotMgr.restoreCacheGroupFiles(req.snapshotName(), grpName, newFiles);
            } catch (IgniteCheckedException e) {
                RestoreSnapshotFuture fut0 = fut;

                if (fut0 != null && fut0.id().equals(req.requestId()))
                    fut0.onDone(e);
            } finally {
                rollbackLock.unlock();
            }
        }
    }

    private void finishPerform(UUID reqId, Map<UUID, SnapshotRestorePerformResponse> map, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        Throwable failure = F.first(errs.values());

        if (failure == null)
            failure = fut0.failure();

        if (failure == null && !map.keySet().containsAll(fut0.request().requiredNodes())) {
            Set<UUID> reqNodes = new HashSet<>(fut0.request().requiredNodes());

            reqNodes.removeAll(map.keySet());

            log.warning("Node left the cluster, snapshot restore operation should be reverted [nodeIds=" + F.concat(reqNodes, ", "));

            fut0.handleError(failure = new IgniteException(new IgniteException(OP_REJECT_MSG +
                "Baseline node has left the cluster [nodeId(s)=" + F.concat(reqNodes, ", ") + ']')));
        }

        if (failure != null) {
            if (U.isLocalNodeCoordinator(ctx.discovery())) {
                log.info("Starting snapshot restore rollback routine.");

                rollbackRestoreProc.start(reqId, new SnapshotRestoreRollbackRequest(fut0.request().requestId(), failure));
            }

            return;
        }

        if (U.isLocalNodeCoordinator(ctx.discovery()))
            ctx.cache().dynamicStartCachesByStoredConf(fut0.startConfigs(), true, true, false, null, true);
    }

    // todo separate rollback request
    private IgniteInternalFuture<SnapshotRestoreRollbackResponse> rollback(SnapshotRestoreRollbackRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        RestoreSnapshotFuture fut0 = fut;

        SnapshotRestorePrepareRequest req0 = fut0.request();

        if (req0 == null || !req.requestId().equals(req0.requestId()))
            return errResponse("Unknown snapshot restore rollback operation was rejected [fut=" + fut + ", req=" + req + ']');

        for (String grpName : req0.groups())
            rollbackChanges(fut0, grpName);

        fut0.handleError(req.reason());

        return new GridFinishedFuture<>(new SnapshotRestoreRollbackResponse());
    }

    private void finishRollback(UUID reqId, Map<UUID, SnapshotRestoreRollbackResponse> map, Map<UUID, Exception> errs) {
        RestoreSnapshotFuture fut0 = fut;

        if (!F.isEmpty(errs)) {
            completeFuture(reqId, errs, fut0);

            return;
        }

        fut0.onDone(fut0.failure());
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     * @param fut Key change future.
     * @return {@code True} if future was completed by this call.
     */
    private boolean completeFuture(UUID reqId, Map<UUID, Exception> err, RestoreSnapshotFuture fut) {
        if (!fut.id().equals(reqId) || fut.isDone())
            return false;

        return !F.isEmpty(err) ? fut.onDone(F.firstValue(err)) : fut.onDone();
    }

    private StoredCacheData unmarshal(IgniteConfiguration cfg, File cacheDataFile) throws IOException, IgniteCheckedException {
        JdkMarshaller marshaller = MarshallerUtils.jdkMarshaller(cfg.getIgniteInstanceName());

        try (InputStream stream = new BufferedInputStream(new FileInputStream(cacheDataFile))) {
            StoredCacheData data = marshaller.unmarshal(stream, U.resolveClassLoader(cfg));

            return data;
        }
    }

    private <T> IgniteInternalFuture<T> errResponse(String msg) {
        return errResponse(new IgniteException(msg));
    }

    private <T> IgniteInternalFuture<T> errResponse(Exception ex) {
        //return errResponse(msg, null);
        return new GridFinishedFuture<>(ex);
    }

    static class PendingStartCacheGroup {
        volatile boolean canRollback = true;

        Set<String> caches = new GridConcurrentHashSet<>();
    }

    static class RestoreOperationContext {
        private final Map<String, List<File>> newGrpFiles = new HashMap<>();

        public List<File> get(String grpName) {
            return newGrpFiles.get(grpName);
        }

        public List<File> remove(String grpName) {
            return newGrpFiles.remove(grpName);
        }

        public boolean isEmpty() {
            return newGrpFiles.isEmpty();
        }

        public void put(String grpName, List<File> files) {
            newGrpFiles.put(grpName, files);
        }
    }

    /** */
    protected static class RestoreSnapshotFuture extends GridFutureAdapter<Void> {
        /** Request ID. */
        private final boolean initiator;

        private final AtomicReference<SnapshotRestorePrepareRequest> reqRef = new AtomicReference<>();

        private final RestoreOperationContext rollbackCtx = new RestoreOperationContext();

        private volatile Throwable err;

        public Throwable failure() {
            return err;
        }

        private volatile Collection<StoredCacheData> cacheCfgsToStart;

        private final Map<String, PendingStartCacheGroup> pendingStartCaches = new ConcurrentHashMap<>();

        public SnapshotRestorePrepareRequest request() {
            return reqRef.get();
        }

        public Set<Integer> cacheIds = new GridConcurrentHashSet<>();

        public boolean containsCache(String name) {
            return cacheIds.contains(CU.cacheId(name));
        }

        public void addCacheId(int cacheId) {
            cacheIds.add(cacheId);
        }

        public boolean request(SnapshotRestorePrepareRequest req) {
            if (!reqRef.compareAndSet(null, req))
                return false;

            for (String grpName : req.groups()) {
                cacheIds.add(CU.cacheId(grpName));

                pendingStartCaches.put(grpName, new PendingStartCacheGroup());
            }

            return true;
        }

        /**
         * @param initiator A flag indicating that the node is the initiator of the request.
         */
        RestoreSnapshotFuture(boolean initiator) {
            this.initiator = initiator;
        }

        public boolean initiator() {
            return initiator;
        }

        /** @return Request ID. */
        public UUID id() {
            SnapshotRestorePrepareRequest req = reqRef.get();

            return req != null ? req.requestId() : null;
        }

        public void handleError(Throwable err) {
            this.err = err;
        }

        public void startConfigs(Collection<StoredCacheData> ccfgs) {
            cacheCfgsToStart = ccfgs;
        }

        public Collection<StoredCacheData> startConfigs() {
            return cacheCfgsToStart;
        }

        public RestoreOperationContext rollbackContext() {
            return rollbackCtx;
        }

        @Override protected boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
            return super.onDone(res, err, cancel);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RestoreSnapshotFuture.class, this);
        }
    }
}
