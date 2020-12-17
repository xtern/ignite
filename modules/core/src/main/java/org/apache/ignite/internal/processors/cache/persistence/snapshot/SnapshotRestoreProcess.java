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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAttributes;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestorePrepareResponse.CacheGroupSnapshotDetails;
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
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT_RESTORE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT_RESTORE;

/**
 * Distributed process to restore cache group from the snapshot.
 */
public class SnapshotRestoreProcess {
    private final GridKernalContext ctx;

    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestorePrepareResponse> prepareRestoreProc;

    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestorePerformResponse> performRestoreProc;

    private final IgniteLogger log;

    private volatile Collection<CacheConfiguration> ccfgs;

    private volatile SnapshotRestoreRequest req = null;

    private volatile RestoreSnapshotFuture fut;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotRestoreProcess(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

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

    private IgniteInternalFuture<SnapshotRestorePrepareResponse> prepare(SnapshotRestoreRequest req) {
        if (inProgress()) {
            // todo do we need it?
            return new GridFinishedFuture<>(new IgniteException("Snapshot restore operation was rejected. " +
                "The previous restore operation was not completed."));
        }

        // todo forbid state change
        // todo forbid node join
        if (!ctx.state().clusterState().state().active())
            throw new IgniteException("Operation was rejected. The cluster should be active.");

        this.req = req;

        List<CacheGroupSnapshotDetails> grpCfgs = new ArrayList<>();

        // read cache configuration
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

        return new GridFinishedFuture<>(new SnapshotRestorePrepareResponse(grpCfgs));
    }

    private CacheGroupSnapshotDetails readCacheGroupDetails(String snapshotName, String grpName) throws IgniteCheckedException, IOException {
        File cacheDir = ctx.cache().context().snapshotMgr().resolveSnapshotCacheDir(snapshotName, ctx.config(), grpName);

        if (!cacheDir.exists())
            return null;

        Set<Integer> parts = new HashSet<>();

        List<CacheConfiguration<?, ?>> cacheCfgs = new ArrayList<>(1);

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

        return new CacheGroupSnapshotDetails(grpName, cacheCfgs, parts);
    }

    private void finishPrepare(UUID reqId, Map<UUID, SnapshotRestorePrepareResponse> res, Map<UUID, Exception> errs) {
        if (!errs.isEmpty()) {
            if (req != null && req.requestId().equals(reqId))
                req = null;

            completeFuture(reqId, errs, fut);
        }

        boolean isInitiator = fut != null && fut.id().equals(reqId);
        boolean isCoordinator = U.isLocalNodeCoordinator(ctx.discovery());

        List<String> notFoundGroups = new ArrayList<>(req.groups());

        try {
            Collection<CacheConfiguration> details = validateResponses(res);

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

            ccfgs = details;
        }
        catch (Exception e) {
            req = null;

            if (isInitiator)
                fut.onDone(e);

            return;
        }

        // todo should be coordinator - how to handle errors
        if (isCoordinator) {
            System.out.println(">xxx> running perform phase...");

            performRestoreProc.start(reqId, req);
        }
    }

    private Collection<CacheConfiguration> validateResponses(Map<UUID, SnapshotRestorePrepareResponse> res) throws IgniteCheckedException {
        Collection<CacheConfiguration> allCfgs = new ArrayList<>();
        Map<String, T2<Map<String, CacheConfiguration>, Set<Integer>>> globalParts = new HashMap<>();

        for (Map.Entry<UUID, SnapshotRestorePrepareResponse> e : res.entrySet()) {
            UUID nodeId = e.getKey();
            SnapshotRestorePrepareResponse resp = e.getValue();

            List<CacheGroupSnapshotDetails> groups = resp.groups();

            if (groups == null)
                continue;

            for (CacheGroupSnapshotDetails grpDetails : groups) {
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

                CacheConfiguration<?, ?> firstCfgInGroup = null;

                for (CacheConfiguration<?, ?> cfg : grpDetails.configs()) {
                    savedGrpEntry.get2().addAll(grpDetails.parts());

                    Map<String, CacheConfiguration> savedCcfgs = savedGrpEntry.get1();

                    CacheConfiguration savedCfg = savedCcfgs.get(cfg.getName());

                    GridCacheAttributes savedAttrs = new GridCacheAttributes(savedCfg, null);
                    GridCacheAttributes currAttr = new GridCacheAttributes(cfg, null);

                    // todo rmt nodeId not local + message formatting
                    //CU.checkCacheAttributes(savedAttrs, currAttr, ctx.localNodeId(), log);

                    if (firstCfgInGroup == null)
                        firstCfgInGroup = cfg;
//                    else
//                        CU.validateCacheGroupConfiguration(firstCfgInGroup, cfg, log);
                }
            }
        }

        for (T2<Map<String, CacheConfiguration>, Set<Integer>> value : globalParts.values()) {
            int reqParts = F.first(value.get1().values()).getAffinity().partitions();
            int availParts = value.get2().size();

            if (reqParts != availParts) // todo name of the group
                throw new IgniteCheckedException("Cannot restore snapshot, not all partitions available [required=" + reqParts + ", avail=" + availParts + "]");
        }

        return allCfgs;
    }

    private IgniteInternalFuture<SnapshotRestorePerformResponse> perform(SnapshotRestoreRequest req) {
        if (!req.equals(this.req))
            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        try {
            if (!ctx.clientNode()) {
                ctx.cache().context().snapshotMgr().restoreCacheGroupsLocal(req.snapshotName(), req.groups());

                return new GridFinishedFuture<>(new SnapshotRestorePerformResponse());
            }
        } catch (IgniteCheckedException e) {
            RestoreSnapshotFuture fut0 = fut;

            if (fut0 != null && fut0.id().equals(req.requestId()))
                fut0.onDone(e);

            return new GridFinishedFuture<>(e);
        } finally {
            this.req = null;
        }

        return new GridFinishedFuture<>(new SnapshotRestorePerformResponse());
    }

    private void finishPerform(UUID reqId, Map<UUID, SnapshotRestorePerformResponse> map, Map<UUID, Exception> errs) {
        try {
            Collection<CacheConfiguration> ccfgs0 = ccfgs;

            ccfgs = null;

            if (!F.isEmpty(errs)) {
                completeFuture(reqId, errs, fut);

                return;
            }

            IgniteInternalFuture<Boolean> startCachesFut = null;

            if (U.isLocalNodeCoordinator(ctx.discovery()))
                startCachesFut = ctx.cache().dynamicStartCaches(ccfgs0, true, true, false);

            if (fut == null || !fut.id().equals(reqId))
                return;

            if (F.isEmpty(ccfgs0)) {
                completeFuture(reqId, errs, fut);

                return;
            }

            // If initiator is the coordinator.
            if (startCachesFut != null) {
                startCachesFut.listen(f -> completeFuture(reqId, errs, fut));

                return;
            }

            ctx.getSystemExecutorService().submit(() -> {
                ensureCachesStarted(ccfgs0);

                completeFuture(reqId, errs, fut);
            });
        } finally {
            req = null;
        }
    }

    private void ensureCachesStarted(Collection<CacheConfiguration> ccfgs) {
        long maxTime = U.currentTimeMillis() + 15_000;

        for (;;) {
            boolean failed = false;

            for (CacheConfiguration<?, ?> cfg : ccfgs) {
                if (failed |= ctx.cache().jcacheProxy(cfg.getName(), true) == null)
                    break;
            }

            if (!failed)
                break;

            if (U.currentTimeMillis() > maxTime) {
                log.warning("Timeout waiting for caches startup.");

                break;
            }

            GridDhtPartitionsExchangeFuture fut0 = ctx.cache().context().exchange().lastTopologyFuture();

            // Exchange didn't started yet.
            if (fut0.isDone()) {
                try {
                    U.sleep(200);
                }
                catch (IgniteInterruptedCheckedException exception) {
                    exception.printStackTrace();
                }

                continue;
            }

            try {
                // todo listen?
                System.out.println(">xxx> waiting topology ");
                fut0.get();
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to wait caches startup.", e);

                break;
            }
        }
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     * @param fut Key change future.
     * @return {@code True} if future was completed by this call.
     */
    private boolean completeFuture(UUID reqId, Map<UUID, Exception> err, RestoreSnapshotFuture fut) {
        ccfgs = null;

        boolean isInitiator = fut != null && fut.id().equals(reqId);

        if (!isInitiator || fut.isDone())
            return false;

        return !F.isEmpty(err) ? fut.onDone(F.firstValue(err)) : fut.onDone();
    }

    private @Nullable List<CacheConfiguration<?, ?>> readStoredCacheConfigs(IgniteConfiguration cfg, File cacheDir) throws IOException, IgniteCheckedException {
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
