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

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Snapshot restore tests.
 */
public class IgniteClusterSnapshoRestoreSelfTest extends AbstractSnapshotSelfTest {
    /** Timeout. */
    private static final long MAX_AWAIT_MILLIS = 15_000;

//    /** Cache configuration for test. */
//    private static final CacheConfiguration<Integer, Integer> atomicCcfg = new CacheConfiguration<Integer, Integer>("atomicCacheName")
//        .setAtomicityMode(CacheAtomicityMode.ATOMIC)
//        .setBackups(2)
//        .setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTS_COUNT));

//    /** {@code true} if node should be started in separate jvm. */
//    protected volatile boolean jvm;

    protected CacheConfiguration[] cacheCfgs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        if (cacheCfgs != null)
            cfg.setCacheConfiguration(cacheCfgs);

        return cfg;
    }

    /** @throws Exception If fails. */
    @Before
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();
    }

    /** {@inheritDoc} */
    @Override public void afterTestSnapshot() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If fails. */
    @Test
    public void testBasicClusterSnapshotRestore() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

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
        int keysCnt = 100;

        String customTypeName = "customType";

        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache cache = ignite.getOrCreateCache(dfltCacheCfg);

        for (int i = 0; i < keysCnt; i++) {
            BinaryObjectBuilder builder = ignite.binary().builder(customTypeName);

            builder.setField("num", i);
            builder.setField("name", String.valueOf(i));

            cache.put(i, builder.build());
        }

//        resetBaselineTopology();

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        // remove metadata
        int typeId = ignite.context().cacheObjects().typeId(customTypeName);

        ignite.context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        cache = ignite.cache(dfltCacheCfg.getName()).withKeepBinary();

        assert cache != null;

        assertEquals(keysCnt, cache.size());

        for (int i = 0; i < keysCnt; i++) {
            BinaryObject obj = (BinaryObject)cache.get(i);

            assertEquals(Integer.valueOf(i), obj.field("num"));
            assertEquals(String.valueOf(i), obj.field("name"));
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreRejectOnInActiveCluster() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

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
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg.setBackups(0), CACHE_KEYS_RANGE);

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

        IgniteEx ignite = startGridsWithCache(nodesCnt - 2, dfltCacheCfg, 0);

        String customTypeName = "customType";

        IgniteCache<Object, Object> cache = ignite.cache(dfltCacheCfg.getName());

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            BinaryObjectBuilder builder = ignite.binary().builder(customTypeName);

            builder.setField("num", i);
            builder.setField("name", String.valueOf(i));

            cache.put(i, builder.build());
        }

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        startGrid(nodesCnt - 2);
        startGrid(nodesCnt - 1);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        cache.destroy();

        awaitPartitionMapExchange();

        // remove metadata
        int typeId = grid(nodesCnt - 1).context().cacheObjects().typeId(customTypeName);

        grid(nodesCnt - 1).context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        awaitPartitionMapExchange();

        cache = grid(nodesCnt - 1).cache(dfltCacheCfg.getName()).withKeepBinary();

        assert cache != null;

        assertEquals(CACHE_KEYS_RANGE, cache.size());

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            BinaryObject obj = (BinaryObject)cache.get(i);

            assertEquals(Integer.valueOf(i), obj.field("num"));
            assertEquals(String.valueOf(i), obj.field("name"));
        }
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

    /** @throws Exception If fails. */
    @Test
    // todo
    @Ignore
    public void testActivateFromClientWhenRestoring() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

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
            assertEquals(i, testCache.get(i));
    }

    private void putKeys(IgniteCache<Object, Object> cache, int startIdx, int cnt) {
        for (int i = startIdx; i < (startIdx + cnt); i++)
            cache.put(i, i);
    }
}
