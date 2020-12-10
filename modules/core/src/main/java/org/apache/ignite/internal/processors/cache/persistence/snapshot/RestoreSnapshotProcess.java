package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreResponse.CacheGroupSnapshotDetails;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT_RESTORE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT_RESTORE;

public class RestoreSnapshotProcess {
    private final GridKernalContext ctx;

    private volatile SnapshotRestoreRequest req;

    private volatile RestoreSnapshotFuture fut;

    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> prepareRestoreProc;

    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> performRestoreProc;

    public RestoreSnapshotProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        prepareRestoreProc = new DistributedProcess<>(ctx, START_SNAPSHOT_RESTORE, this::prepare, this::finishPrepare);
        performRestoreProc = new DistributedProcess<>(ctx, END_SNAPSHOT_RESTORE, this::perform, this::finishPerform);
    }

    // validating
    private IgniteInternalFuture<SnapshotRestoreResponse> prepare(SnapshotRestoreRequest req) {
//        if (ctx.clientNode())
//            return new GridFinishedFuture<>();

        if (inProgress()) {
            // todo do we need it?
            return new GridFinishedFuture<>(new IgniteException("Snapshot restore operation was rejected. " +
                "The previous restore was not completed."));
        }

        SnapshotRestoreResponse response = new SnapshotRestoreResponse();

        // todo forbid state change
        // todo forbid node join
//        if (ctx.state().clusterState().state() != ClusterState.ACTIVE_READ_ONLY)
//            throw new IgniteException("Operation was rejected. The cluster should be in read-only mode.");

        this.req = req;

        // read cache configuration
        for (String cacheName : req.groups()) {
            StoredCacheData data;

            try {
                data = readStoredCacheConfig(ctx.config(), req.snapshotName(), cacheName);
            }
            catch (IOException | IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }

            if (data == null)
                continue;

            File cacheDir = ctx.cache().context().snapshotMgr().resolveSnapshotCacheDir(req.snapshotName(), ctx.config(), cacheName);
            // todo redundant
            if (!cacheDir.exists())
                continue;

            Set<Integer> parts = new HashSet<>();

            for (String fileName : cacheDir.list((dir, name) -> name.startsWith(FilePageStoreManager.PART_FILE_PREFIX))) {
                int partId = Integer.parseInt(fileName.substring(FilePageStoreManager.PART_FILE_PREFIX.length(), fileName.indexOf('.')));

                parts.add(partId);
            }

            response.put(cacheName, data.config(), parts);
        }

        return new GridFinishedFuture<>(response);
    }

    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestoreResponse> res, Map<UUID, Exception> errs) {
        if (!errs.isEmpty()) {
            if (req != null && req.requestId().equals(reqId))
                req = null;

            completeFuture(reqId, errs, fut);
        }

        boolean isInitiator = fut != null && fut.id().equals(reqId);
        boolean isCoordinator = U.isLocalNodeCoordinator(ctx.discovery());

//        if (isInitiator || isCoordinator) {
        Collection<CacheGroupSnapshotDetails> details;

        try {
            details = validateResponses(res);
        } catch (Exception e) {
            req = null;

            if (isInitiator)
                fut.onDone(e);

            return;
        }

        // todo should be coordinator - how to handle errors
        if (isInitiator) {
            System.out.println(">xxx> stop caches...");

            ctx.getSystemExecutorService().submit(() -> {
                ctx.cache().dynamicDestroyCaches(req.groups(), true, false).listen(f -> {
                    if (f.error() == null && !f.isCancelled()) {
                        System.out.println(">xxx> start perform...");
                        performRestoreProc.start(reqId, req);
                    } else
                        if (f.error() != null)
                            fut.onDone(f.error());
                    // todo handle errors, finish future
                });
            });
        }
    }

    private Collection<CacheGroupSnapshotDetails> validateResponses(Map<UUID, SnapshotRestoreResponse> res) throws IgniteCheckedException {
        Map<String, CacheGroupSnapshotDetails> globalParts = new HashMap<>();

        for (Map.Entry<UUID, SnapshotRestoreResponse> e : res.entrySet()) {
            UUID nodeId = e.getKey();
            SnapshotRestoreResponse resp = e.getValue();

            Map<String, CacheGroupSnapshotDetails> parts = resp.locParts();

            if (parts == null)
                continue;

            for (CacheGroupSnapshotDetails cacheDetails : parts.values()) {
                CacheConfiguration<?, ?> lastCfg = cacheDetails.config();
                String cacheName = lastCfg.getName();
                CacheGroupSnapshotDetails details = globalParts.get(cacheName);

                // todo store source nodeId to show correct error message
                if (details == null) {
                    details = new CacheGroupSnapshotDetails(lastCfg, new HashSet<>());

                    globalParts.put(cacheName, details);

                    continue;
                }

                CacheConfiguration<?, ?> firstCfg = details.config();

                // todo full validation
                A.ensure(F.eq(firstCfg.getCacheMode(), lastCfg.getCacheMode()), "Cache mode mismatch [cache=" + cacheName + ", exp=" +
                    firstCfg.getCacheMode() + ", atcual=" + lastCfg.getCacheMode() + "]");

                A.ensure(F.eq(firstCfg.getBackups(), lastCfg.getBackups()), "Number of backups mismatch [cache=" + cacheName + ", exp=" +
                    firstCfg.getBackups() + ", atcual=" + lastCfg.getBackups() + "]");

                A.ensure(F.eq(firstCfg.getAtomicityMode(), lastCfg.getAtomicityMode()), "Atomicity mode mismatch [cache=" + cacheName + ", exp=" +
                    firstCfg.getAtomicityMode() + ", atcual=" + lastCfg.getAtomicityMode() + "]");

                details.parts().addAll(cacheDetails.parts());
            }
        }

        for (CacheGroupSnapshotDetails cacheDetails : globalParts.values()) {
            int reqParts = cacheDetails.config().getAffinity().partitions();
            int availParts = cacheDetails.parts().size();

            if (reqParts != availParts)
                throw new IgniteCheckedException("Cannot restore snapshot, not all partitions available [required=" + reqParts + ", avail=" + availParts + "]");
        }

        return globalParts.values();
    }

    public IgniteFuture<Void> start(String snpName, Collection<String> cacheOrGrpNames) {
//        if (ctx.state().clusterState().state() != ClusterState.ACTIVE_READ_ONLY)
//            return new IgniteFinishedFutureImpl<>(new IgniteException("Snapshot restore operation was rejected. " +
//                " The cluster should be in read-only mode."));

        IgniteInternalFuture<Void> fut0 = fut;

        if (fut0 != null && !fut0.isDone()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Snapshot restore operation was rejected. " +
                "The previous snapshot restore operation was not completed."));
        }

        SnapshotRestoreRequest req = new SnapshotRestoreRequest(snpName, cacheOrGrpNames);

        // todo cas?
        fut = new RestoreSnapshotFuture(req.requestId());

        prepareRestoreProc.start(req.requestId(), req);

        try {
            ctx.cache().dynamicDestroyCaches(cacheOrGrpNames, true, false).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return new IgniteFutureImpl<>(fut);
    }

    public boolean inProgress() {
        return req != null;
    }

    private IgniteInternalFuture<SnapshotRestoreResponse> perform(SnapshotRestoreRequest req) {
//        this.req = req;
        if (!req.equals(this.req))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        SnapshotRestoreResponse resp = new SnapshotRestoreResponse();

        try {
            if (!ctx.clientNode()) {
                Collection<CacheGroupDescriptor> grps = ctx.cache().context().snapshotMgr().restoreCacheGroupsLocal(req.snapshotName(), req.groups());

//                RestoreSnapshotFuture fut0 = fut;

//                if (fut0 != null && fut0.id().equals(req.requestId())) {
                    for (CacheGroupDescriptor grp : grps)
                        resp.put(grp.cacheOrGroupName(), grp.config(), null);
//                }
            }


        } catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        } finally {
            this.req = null;
        }

        return new GridFinishedFuture<>(resp);
    }

    private void finishPerform(UUID reqId, Map<UUID, SnapshotRestoreResponse> map, Map<UUID, Exception> errs) {
        try {
            // remap

            Map<String, CacheConfiguration> resMap = new HashMap<>();

            for (SnapshotRestoreResponse nodeResp : map.values()) {
                for (Map.Entry<String, CacheGroupSnapshotDetails> entry : nodeResp.locParts().entrySet()) {
                    resMap.put(entry.getKey(), entry.getValue().config());
                }

            }

            // todo should be coordinator
            if (completeFuture(reqId, errs, fut)) {

                for (Map.Entry<String, CacheConfiguration> entry : resMap.entrySet()) {
                    // todo batch - wait start
                    ctx.cache().dynamicStartCache(entry.getValue(),
                        entry.getKey(),
                        null,
                        false,
                        true,
                        true);
                }
//                ctx.cache().dynamicStartCache(grp.config(),
//                    grp.cacheOrGroupName(),
//                    null,
//                    false,
//                    true,
//                    true).get();
            }
//                ctx.cache().dynamicStartCache(cacheOrGrpNames, true, false).get();

        } finally {
            this.req = null;
        }
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     * @param fut Key change future.
     * @return {@code True} if future was completed by this call.
     */
    private boolean completeFuture(UUID reqId, Map<UUID, Exception> err, RestoreSnapshotFuture fut) {
        boolean isInitiator = fut != null && fut.id().equals(reqId);

        if (!isInitiator || fut.isDone())
            return false;

        return !F.isEmpty(err) ? fut.onDone(F.firstValue(err)) : fut.onDone();
    }

    private @Nullable StoredCacheData readStoredCacheConfig(IgniteConfiguration cfg, String snpName, String cacheName) throws IOException, IgniteCheckedException {
        File cacheDir = ctx.cache().context().snapshotMgr().resolveSnapshotCacheDir(snpName, cfg, cacheName);

        File cacheDataFile = new File(cacheDir, FilePageStoreManager.CACHE_DATA_FILENAME);

        if (!cacheDataFile.exists())
            return null;

        JdkMarshaller marshaller = MarshallerUtils.jdkMarshaller(cfg.getIgniteInstanceName());

        try (InputStream stream = new BufferedInputStream(new FileInputStream(cacheDataFile))) {
            return marshaller.unmarshal(stream, U.resolveClassLoader(cfg));
        }
    }

    protected static class RestoreSnapshotFuture extends GridFutureAdapter<Void> {
        /** Request ID. */
        private final UUID id;

        /** @param id Request ID. */
        RestoreSnapshotFuture(UUID id) {
            this.id = id;
        }

        /** @return Request ID. */
        public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RestoreSnapshotFuture.class, this);
        }
    }
}
