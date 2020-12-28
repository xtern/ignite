/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Snapshot restore tests.
 */
public class IgniteClusterSnapshoRestoreSelfTest extends AbstractSnapshotSelfTest {
    /** Timeout. */
    private static final long MAX_AWAIT_MILLIS = 15_000;

    protected CacheConfiguration[] cacheCfgs;

    protected Function<Integer, Object> valueBuilder = new IntValueBuilder();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        if (cacheCfgs != null)
            cfg.setCacheConfiguration(cacheCfgs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void afterTestSnapshot() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If fails. */
    @Test
    public void testBasicClusterSnapshotRestore() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testBasicClusterSnapshotRestoreWithMetadata() throws Exception {
        String customTypeName = "customType";

        valueBuilder = new BinaryValueBuilder(0, customTypeName);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        // remove metadata
        int typeId = ignite.context().cacheObjects().typeId(customTypeName);

        ignite.context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()).withKeepBinary(), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreRejectOnInActiveCluster() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cluster().state(ClusterState.INACTIVE);

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(MAX_AWAIT_MILLIS), IgniteException.class, "The cluster should be active");
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreWithMissedPartitions() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg.setBackups(0));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        forceCheckpoint();

        stopGrid(1);

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(MAX_AWAIT_MILLIS), IgniteCheckedException.class, "not all partitions available");

        startGrid(1);

        IgniteFuture<Void> fut1 = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut1.get(MAX_AWAIT_MILLIS), IllegalStateException.class, "Cache group \"" + dfltCacheCfg.getName() + "\" should be destroyed manually");

        ignite.cache(dfltCacheCfg.getName()).destroy();

        // todo remove files before restore snapshot?
        awaitPartitionMapExchange();

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreDiffTopology() throws Exception {
        int nodesCnt = 4;

        String customTypeName = "customType";

        valueBuilder = new BinaryValueBuilder(0, customTypeName);

        IgniteEx ignite = startGridsWithCache(nodesCnt - 2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        startGrid(nodesCnt - 2);
        startGrid(nodesCnt - 1);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        // remove metadata
        int typeId = grid(nodesCnt - 1).context().cacheObjects().typeId(customTypeName);

        grid(nodesCnt - 1).context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        awaitPartitionMapExchange();

        checkCacheKeys(grid(nodesCnt - 1).cache(dfltCacheCfg.getName()).withKeepBinary(), CACHE_KEYS_RANGE);
    }

    @Test
    public void testRestoreSharedCacheGroup() throws Exception {
        String grpName = "shared";
        String cacheName1 = "cache1";
        String cacheName2 = "cache2";

        CacheConfiguration<?, ?> cacheCfg1 = txCacheConfig(new CacheConfiguration<>(cacheName1)).setGroupName(grpName);
        CacheConfiguration<?, ?> cacheCfg2 = txCacheConfig(new CacheConfiguration<>(cacheName2))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC).setGroupName(grpName);

        cacheCfgs = new CacheConfiguration[] {cacheCfg1, cacheCfg2};

        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache1 = ignite.cache(cacheName1);
        IgniteCache<Object, Object> cache2 = ignite.cache(cacheName2);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        cache1.destroy();

        awaitPartitionMapExchange();

        // todo should wait for exchange if group descriptor doesn't exists
        awaitPartitionMapExchange();

        IgniteSnapshotManager snapshotMgr = ignite.context().cache().context().snapshotMgr();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                snapshotMgr.restoreCacheGroups(SNAPSHOT_NAME, Arrays.asList(cacheName1, cacheName2)).get(MAX_AWAIT_MILLIS);

                return null;
            },
            IllegalArgumentException.class,
            "Cache group(s) \"" + cacheName1 + ", " + cacheName2 + "\" not found in snapshot \"" + SNAPSHOT_NAME + "\""
        );

        cache2.destroy();

        awaitPartitionMapExchange();

        snapshotMgr.restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(grpName)).get(MAX_AWAIT_MILLIS);
    }

    @Test
    public void testIncompatibleMetasUpdate() throws Exception {
        String customTypeName = "customType";

        valueBuilder = new BinaryValueBuilder(0, customTypeName);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        int typeId = ignite.context().cacheObjects().typeId(customTypeName);

        ignite.context().cacheObjects().removeType(typeId);

        BinaryObject[] objs = new BinaryObject[CACHE_KEYS_RANGE];

        IgniteCache<Integer, Object> cache1 = createCacheWithBinaryType(ignite, "cache1", n -> {
            BinaryObjectBuilder builder = ignite.binary().builder(customTypeName);

            builder.setField("id", n);

            objs[n] = builder.build();

            return objs[n];
        });

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        fut.get(MAX_AWAIT_MILLIS);

        // Ensure that existing type has been updated
        BinaryType type = ignite.context().cacheObjects().metadata(typeId);

        assertTrue(type.fieldNames().contains("name"));

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(objs[i], cache1.get(i));

        cache1.destroy();

        grid(0).cache(dfltCacheCfg.getName()).destroy();

        ignite.context().cacheObjects().removeType(typeId);

        // Create cache with incompatible binary type
        cache1 = createCacheWithBinaryType(ignite, "cache1", n -> {
            BinaryObjectBuilder builder = ignite.binary().builder(customTypeName);

            builder.setField("id", UUID.randomUUID());

            objs[n] = builder.build();

            return objs[n];
        });

        final IgniteFuture<Void> fut0 = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> fut0.get(MAX_AWAIT_MILLIS),
            IgniteCheckedException.class,
            "Operation has been rejected, incompatible binary types found"
        );

        ensureCacheDirEmpty(2, dfltCacheCfg.getName());

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(objs[i], cache1.get(i));
    }

    private IgniteCache<Integer, Object> createCacheWithBinaryType(Ignite ignite, String cacheName, Function<Integer, BinaryObject> valBuilder) {
        IgniteCache<Integer, Object> cache = ignite.createCache(new CacheConfiguration<>(cacheName)).withKeepBinary();

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            cache.put(i, valBuilder.apply(i));

        return cache;
    }

    /** @throws Exception If fails. */
    @Test
    public void testRollbackOnNodeFail() throws Exception {
        doRollbackOnNodeFail(DistributedProcess.DistributedProcessType.END_SNAPSHOT_RESTORE);
    }

    private void doRollbackOnNodeFail(DistributedProcess.DistributedProcessType procType) throws Exception {
        IgniteEx ignite = startGridsWithCache(4, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        IgniteSnapshotManager snapshotMgr = ignite.context().cache().context().snapshotMgr();

        // todo block distribprocess and try to activate cluster
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(3));

        spi.blockMessages((node, msg) ->
            msg instanceof SingleNodeMessage && ((SingleNodeMessage<?>)msg).type() == procType.ordinal());

        IgniteFuture<Void> fut = snapshotMgr.restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        spi.waitForBlocked();

        runAsync(() -> {
            stopGrid(3, true);
        });

//        spi.stopBlock();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> fut.get(MAX_AWAIT_MILLIS),
            IgniteCheckedException.class,
            "Operation has been rejected, cluster topology has been changed."
        );

        ensureCacheDirEmpty(3, dfltCacheCfg.getName());
    }

    private void ensureCacheDirEmpty(int nodesCnt, String cacheName) throws IgniteCheckedException {
        for (int nodeIdx = 0; nodeIdx < nodesCnt; nodeIdx++) {
            IgniteEx grid = grid(nodeIdx);

            File dir = resolveCacheDir(grid, cacheName);

            String errMsg = String.format("%s, dir=%s, exists=%b, files=%s",
                grid.name(), dir, dir.exists(), Arrays.toString(dir.list()));

            assertTrue(errMsg, !dir.exists() || dir.list().length == 0);
        }
    }

    private File resolveCacheDir(IgniteEx ignite, String cacheOrGrpName) throws IgniteCheckedException {
        File workDIr = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeDirName = ignite.context().pdsFolderResolver().resolveFolders().folderName() + File.separator;

        File cacheDir = new File(workDIr, nodeDirName + CACHE_DIR_PREFIX + cacheOrGrpName);

        if (cacheDir.exists())
            return cacheDir;

        return new File(workDIr, nodeDirName + CACHE_GRP_DIR_PREFIX + cacheOrGrpName);
    }


    /** @throws Exception If fails. */
    @Test
    // todo
    @Ignore
    public void testActivateFromClientWhenRestoring() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        IgniteEx client = startClientGrid("client");

        client.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(client.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        client.cluster().state(ClusterState.INACTIVE);

        IgniteSnapshotManager snapshotMgr = grid(1).context().cache().context().snapshotMgr();

        // todo block distribprocess and try to activate cluster
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> {
            if (msg instanceof SingleNodeMessage)
                return true;

            System.out.println(">xxx> " + node.id());

            return false;
        });

        IgniteFuture<Void> fut = snapshotMgr.restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        spi.waitForBlocked();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                client.cluster().state(ClusterState.ACTIVE);

                return null;
            },
            IllegalStateException.class,
            "The cluster cannot be activated until the snapshot restore operation is complete."
        );

        spi.stopBlock();

        fut.get(MAX_AWAIT_MILLIS);

        client.cluster().state(ClusterState.ACTIVE);

        checkCacheKeys(client.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

//    @Test
//    public void testPreventRecoveryOnRestoredCacheGroup() throws Exception {
//        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);
//
//        resetBaselineTopology();
//
//        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);
//
//        enableCheckpoints(G.allGrids(), false);
//
//        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);
//
//        stopAllGrids();
//
//        ignite = startGrid(0);
//        startGrid(1);
//
//        ignite.cluster().state(ClusterState.ACTIVE);
//
//        ignite.cache(dfltCacheCfg.getName()).destroy();
//
//        awaitPartitionMapExchange();
//
//        ignite.context().cache().context().snapshotMgr().
//            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);
//
//        stopAllGrids();
//
//        ignite = startGrid(0);
//        startGrid(1);
//
//        ignite.cluster().state(ClusterState.ACTIVE);
//
//        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
//    }

    private void checkCacheKeys(IgniteCache<Object, Object> testCache, int keysCnt) {
        assertEquals(keysCnt, testCache.size());

        for (int i = 0; i < keysCnt; i++)
            assertEquals(valueBuilder.apply(i), testCache.get(i));
    }

    private void putKeys(IgniteCache<Object, Object> cache, int startIdx, int cnt) {
        for (int i = startIdx; i < (startIdx + cnt); i++)
            cache.put(i, i);
    }

    private class IntValueBuilder implements Function<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(Integer key) {
            return key;
        }
    }

    private class BinaryValueBuilder implements Function<Integer, Object> {
        private final int nodeIdx;
        private final String typeName;

        BinaryValueBuilder(int nodeIdx, String typeName) {
            this.nodeIdx = nodeIdx;
            this.typeName = typeName;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Integer key) {
            BinaryObjectBuilder builder = grid(nodeIdx).binary().builder(typeName);

            builder.setField("id", key);
            builder.setField("name", String.valueOf(key));

            return builder.build();
        }
    }
}
