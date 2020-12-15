package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreResponse.CacheGroupSnapshotDetails;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT_RESTORE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT_RESTORE;

public class RestoreSnapshotProcess {
    private final GridKernalContext ctx;

    private volatile SnapshotRestoreRequest req;

    private volatile RestoreSnapshotFuture fut;

    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> prepareRestoreProc;

    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> performRestoreProc;

    private volatile Collection<CacheConfiguration> ccfgs;

    public RestoreSnapshotProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        prepareRestoreProc = new DistributedProcess<>(ctx, START_SNAPSHOT_RESTORE, this::prepare, this::finishPrepare);
        performRestoreProc = new DistributedProcess<>(ctx, END_SNAPSHOT_RESTORE, this::perform, this::finishPerform);
    }

    public IgniteFuture<Void> start(String snpName, Collection<String> cacheOrGrpNames) {
        if (!ctx.state().clusterState().state().active()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Snapshot restore operation was rejected. " +
                "The cluster should be active."));
        }

        IgniteInternalFuture<Void> fut0 = fut;

        if (fut0 != null && !fut0.isDone()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Snapshot restore operation was rejected. " +
                "The previous snapshot restore operation was not completed."));
        }

        SnapshotRestoreRequest req = new SnapshotRestoreRequest(snpName, cacheOrGrpNames);

        // todo cas?
        fut = new RestoreSnapshotFuture(req.requestId());

        prepareRestoreProc.start(req.requestId(), req);

        return new IgniteFutureImpl<>(fut);
    }

    public boolean inProgress() {
        return req != null;
    }

    // validating
    private IgniteInternalFuture<SnapshotRestoreResponse> prepare(SnapshotRestoreRequest req) {
        if (inProgress()) {
            // todo do we need it?
            return new GridFinishedFuture<>(new IgniteException("Snapshot restore operation was rejected. " +
                "The previous restore was not completed."));
        }

        SnapshotRestoreResponse response = new SnapshotRestoreResponse();

        // todo forbid state change
        // todo forbid node join
        if (!ctx.state().clusterState().state().active())
            throw new IgniteException("Operation was rejected. The cluster should be active.");

        this.req = req;

        // read cache configuration
        for (String cacheName : req.groups()) {

            try {
                CacheGroupSnapshotDetails details0 = cacheGroupDetails(req.snapshotName(), cacheName);

                if (details0 != null)
                    response.put(cacheName, details0.configs(), details0.parts());
            }
            catch (IOException | IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }

        return new GridFinishedFuture<>(response);
    }

    private CacheGroupSnapshotDetails cacheGroupDetails(String snapshotName, String grpName) throws IgniteCheckedException, IOException {
        List<CacheConfiguration<?, ?>> ccfgs = readStoredCacheConfigs(ctx.config(), snapshotName, grpName);

        if (ccfgs == null)
            return null;

        File cacheDir = ctx.cache().context().snapshotMgr().resolveSnapshotCacheDir(snapshotName, ctx.config(), grpName);
        // todo redundant
        if (!cacheDir.exists())
            return null;

        Set<Integer> parts = new HashSet<>();

        for (String fileName : cacheDir.list((dir, name) -> name.startsWith(FilePageStoreManager.PART_FILE_PREFIX))) {
            int partId = Integer.parseInt(fileName.substring(FilePageStoreManager.PART_FILE_PREFIX.length(), fileName.indexOf('.')));

            parts.add(partId);
        }

        return new CacheGroupSnapshotDetails(ccfgs, parts);
    }

    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestoreResponse> res, Map<UUID, Exception> errs) {
        if (!errs.isEmpty()) {
            if (req != null && req.requestId().equals(reqId))
                req = null;

            completeFuture(reqId, errs, fut);
        }

        boolean isInitiator = fut != null && fut.id().equals(reqId);
        boolean isCoordinator = U.isLocalNodeCoordinator(ctx.discovery());

        List<String> notFoundGroups = new ArrayList<>(req.groups());

//        if (isInitiator || isCoordinator) {
            Collection<CacheConfiguration> details;

            try {
                details = validateResponses(res);

                // Ensure cahe does not exists
                for (CacheConfiguration e : details) {
                    // todo properly handle cache groups
                    String cacheName = F.isEmpty(e.getGroupName()) ? e.getName() : e.getGroupName();

                    CacheGroupDescriptor desc = ctx.cache().cacheGroupDescriptor(CU.cacheId(cacheName));

                    if (desc != null)
                        throw new IllegalStateException("Cache group \"" + desc.cacheOrGroupName() + "\" should be destroyed manually before perform restore operation.");

                    notFoundGroups.remove(cacheName);
                }

                if (!notFoundGroups.isEmpty())
                    throw new IllegalArgumentException("Cache group(s) \"" + F.concat(notFoundGroups, ", ") + "\" not found in snapshot \"" + req.snapshotName() + "\"");

                this.ccfgs = details;
            }
            catch (Exception e) {
                req = null;

                if (isInitiator)
                    fut.onDone(e);

                return;
            }
//        }

        // todo should be coordinator - how to handle errors
        if (isCoordinator) {
            System.out.println(">xxx> running perform phase...");

            performRestoreProc.start(reqId, req);
//            ctx.getSystemExecutorService().submit(() -> {
//                ctx.cache().dynamicDestroyCaches(req.groups(), true, false).listen(f -> {
//                    if (f.error() == null && !f.isCancelled()) {
//                        System.out.println(">xxx> start perform...");
//                        performRestoreProc.start(reqId, req);
//                    } else
//                        if (f.error() != null)
//                            fut.onDone(f.error());
//                    // todo handle errors, finish future
//                });
//            });
        }
    }

    private Collection<CacheConfiguration> validateResponses(Map<UUID, SnapshotRestoreResponse> res) throws IgniteCheckedException {
//        Map<String, CacheGroupSnapshotDetails> globalParts = new HashMap<>();

        //Map<String, CacheConfiguration> globalCfgs = new HashMap<>();
        Collection<CacheConfiguration> allCfgs = new ArrayList<>();
        Map<String, T2<Map<String, CacheConfiguration>, Set<Integer>>> globalParts = new HashMap<>();

        for (Map.Entry<UUID, SnapshotRestoreResponse> e : res.entrySet()) {
            UUID nodeId = e.getKey();
            SnapshotRestoreResponse resp = e.getValue();

            Map<String, CacheGroupSnapshotDetails> parts = resp.locParts();

            if (parts == null)
                continue;

            for (CacheGroupSnapshotDetails grpDetails : parts.values()) {
                CacheConfiguration firstCfg = F.first(grpDetails.configs());

                if (firstCfg == null)
                    continue;

                String grpName = F.isEmpty(firstCfg.getGroupName()) ? firstCfg.getName() : firstCfg.getGroupName();

                T2<Map<String, CacheConfiguration>, Set<Integer>> savedGrpEntry = globalParts.get(grpName);

                if (savedGrpEntry == null) {
                    savedGrpEntry = new T2<>(new HashMap<>(), new HashSet<>());

                    for (CacheConfiguration<?, ?> cfg : grpDetails.configs()) {
                        savedGrpEntry.get1().put(cfg.getName(), cfg);

                        allCfgs.add(cfg);
                    }

                    savedGrpEntry.get2().addAll(grpDetails.parts());

                    globalParts.put(grpName, savedGrpEntry);

                    continue;
                }

                // todo detailed info
                if (savedGrpEntry.get1().size() != grpDetails.configs().size())
                    throw new IllegalStateException("Count of caches in shared groups mismatch");

                for (CacheConfiguration<?, ?> cfg : grpDetails.configs()) {
                    savedGrpEntry.get2().addAll(grpDetails.parts());

                    Map<String, CacheConfiguration> savedCcfgs = savedGrpEntry.get1();

                    CacheConfiguration savedCfg = savedCcfgs.get(cfg.getName());

                    // todo full validation
                    A.ensure(F.eq(savedCfg.getCacheMode(), cfg.getCacheMode()), "Cache mode mismatch [grp=" + grpName + ", exp=" +
                        savedCfg.getCacheMode() + ", atcual=" + cfg.getCacheMode() + "]");

                    A.ensure(F.eq(savedCfg.getBackups(), cfg.getBackups()), "Number of backups mismatch [grp=" + grpName + ", exp=" +
                        savedCfg.getBackups() + ", atcual=" + cfg.getBackups() + "]");

                    A.ensure(F.eq(savedCfg.getAtomicityMode(), cfg.getAtomicityMode()), "Atomicity mode mismatch [grp=" + grpName + ", exp=" +
                        savedCfg.getAtomicityMode() + ", atcual=" + cfg.getAtomicityMode() + "]");

                    // todo grpName
                    // todo parttitions count
//
//
//                    T2<Map<String, CacheConfiguration>, Set<Integer>> entry = globalParts.get(grpName);
//
//                    if (entry == null) {
//                        entry = new T2<>(new HashMap<>(), new HashSet<>());
//
//                        globalParts.put(grpName, entry);
//
//                        skipValidation = true;
//                    }
//
//                    if (skipValidation) {
//                        entry.get1().put(cfg.getName(), cfg);
//
//                        entry.get2().addAll(grpDetails.parts());
//                    }
//
//                    Map<String, CacheConfiguration> cacheCfgMap = entry.get1();
//
//                    CacheConfiguration savedCfg = cacheCfgMap.get(cfg.getName());
//
//                    if (savedCfg == null)
                }

//                if (ccfgs)
//
//                CacheConfiguration<?, ?> lastCfg = grpDetails.config();
//                String cacheName = lastCfg.getName();
//                CacheGroupSnapshotDetails details = globalParts.get(cacheName);
//
//                // todo store source nodeId to show correct error message
//                if (details == null) {
//                    details = new CacheGroupSnapshotDetails(lastCfg, new HashSet<>());
//
//                    globalParts.put(cacheName, details);
//
//                    continue;
//                }
//
//                CacheConfiguration<?, ?> firstCfg = details.config();
//
//
//
//                details.parts().addAll(grpDetails.parts());
            }
        }

        for (T2<Map<String, CacheConfiguration>, Set<Integer>> value : globalParts.values()) {
            int reqParts = F.first(value.get1().values()).getAffinity().partitions();
            int availParts = value.get2().size();

            if (reqParts != availParts) // todo name of group
                throw new IgniteCheckedException("Cannot restore snapshot, not all partitions available [required=" + reqParts + ", avail=" + availParts + "]");
        }

//        for (CacheGroupSnapshotDetails cacheDetails : globalParts.values()) {
//            int reqParts = cacheDetails.config().getAffinity().partitions();
//            int availParts = cacheDetails.parts().size();
//
//            if (reqParts != availParts)
//                throw new IgniteCheckedException("Cannot restore snapshot, not all partitions available [required=" + reqParts + ", avail=" + availParts + "]");
//        }

        return allCfgs;
    }

    private IgniteInternalFuture<SnapshotRestoreResponse> perform(SnapshotRestoreRequest req) {
        if (!req.equals(this.req))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        try {
            if (!ctx.clientNode()) {
                ctx.cache().context().snapshotMgr().restoreCacheGroupsLocal(req.snapshotName(), req.groups());

                return new GridFinishedFuture<>(new SnapshotRestoreResponse());
            }
        } catch (IgniteCheckedException e) {
            RestoreSnapshotFuture fut0 = fut;

            if (fut0 != null && fut0.id().equals(req.requestId()))
                fut0.onDone(e);

            return new GridFinishedFuture<>(e);
        } finally {
            this.req = null;
        }

        return new GridFinishedFuture<>(new SnapshotRestoreResponse());
    }

    private void finishPerform(UUID reqId, Map<UUID, SnapshotRestoreResponse> map, Map<UUID, Exception> errs) {
        try {
            // remap

//            Map<String, CacheConfiguration> resMap = new HashMap<>();
//
//            for (SnapshotRestoreResponse nodeResp : map.values()) {
//                for (Map.Entry<String, CacheGroupSnapshotDetails> entry : nodeResp.locParts().entrySet()) {
//                    resMap.put(entry.getKey(), entry.getValue().config());
//                }
//
//            }

            if (!F.isEmpty(errs)) {
                completeFuture(reqId, errs, fut);

                // todo log errors
            }

            if (U.isLocalNodeCoordinator(ctx.discovery())) {
                ctx.cache().dynamicStartCaches(ccfgs, true, true, false);

//                for (CacheConfiguration<?, ?> cfg : ccfgs) {
//                    // todo batch - wait start
//
//                    ctx.cache().dynamicStartCache(cfg,
//                        cfg.getName(),
//                        null,
//                        false,
//                        true,
//                        true);
//                }
            }

            if (fut != null && fut.id().equals(reqId)) {
                ctx.getSystemExecutorService().submit(() -> {
                    long maxTime = U.currentTimeMillis() + 15_000;

                    boolean success;

                    do {
                        success = true;

                        for (CacheConfiguration cfg : ccfgs) {
                            IgniteCacheProxyImpl<?, ?> proxy = ctx.cache().jcacheProxy(cfg.getName(), true);

                            if (proxy == null) {
                                success = false;

                                try {
                                    U.sleep(200);
                                }
                                catch (IgniteInterruptedCheckedException exception) {
                                    exception.printStackTrace();
                                }

                                break;
                            }
                        }
                    }
                    while (!success && U.currentTimeMillis() <= maxTime);

                    completeFuture(reqId, errs, fut);
                });
            }

//            // todo should be coordinator
//            if (completeFuture(reqId, errs, fut)) {
//
//                for (Map.Entry<String, CacheConfiguration> entry : resMap.entrySet()) {
//                    // todo batch - wait start
//                    ctx.cache().dynamicStartCache(entry.getValue(),
//                        entry.getKey(),
//                        null,
//                        false,
//                        true,
//                        true);
//                }
////                ctx.cache().dynamicStartCache(grp.config(),
////                    grp.cacheOrGroupName(),
////                    null,
////                    false,
////                    true,
////                    true).get();
//            }
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

    private @Nullable List<CacheConfiguration<?, ?>> readStoredCacheConfigs(IgniteConfiguration cfg, String snpName, String cacheName) throws IOException, IgniteCheckedException {
        File cacheDir = ctx.cache().context().snapshotMgr().resolveSnapshotCacheDir(snpName, cfg, cacheName);

        if (!cacheDir.exists())
            return null;

        if (cacheDir.getName().startsWith(CACHE_DIR_PREFIX)) {
            File cacheDataFile = new File(cacheDir, CACHE_DATA_FILENAME);

            assert cacheDataFile.exists() : cacheDataFile;

            return Collections.singletonList(unmarshal(cfg, cacheDataFile));
        }

        List<CacheConfiguration<?, ?>> ccfgs = new ArrayList<>();

        File[] files = cacheDir.listFiles();

        if (files == null)
            return null;

        for (File file : files) {
            if (!file.isDirectory() && file.getName().endsWith(CACHE_DATA_FILENAME) && file.length() > 0)
                ccfgs.add(unmarshal(cfg, file));
        }

        return ccfgs;
    }

    private CacheConfiguration<?, ?> unmarshal(IgniteConfiguration cfg, File cacheDataFile) throws IOException, IgniteCheckedException {
        JdkMarshaller marshaller = MarshallerUtils.jdkMarshaller(cfg.getIgniteInstanceName());

        try (InputStream stream = new BufferedInputStream(new FileInputStream(cacheDataFile))) {
            StoredCacheData data = marshaller.unmarshal(stream, U.resolveClassLoader(cfg));

            return data.config();
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
