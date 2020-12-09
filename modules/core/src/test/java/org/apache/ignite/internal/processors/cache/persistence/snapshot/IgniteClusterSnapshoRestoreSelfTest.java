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

import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
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

//    /** {@inheritDoc} */
//    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(name);
//
//        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
//
//        return cfg;
//    }

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

        resetBaselineTopology();

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        forceCheckpoint();

        ignite.cluster().state(ClusterState.INACTIVE);

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        ignite.cluster().state(ClusterState.ACTIVE);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreRejectOnActiveCluster() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        forceCheckpoint();

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(MAX_AWAIT_MILLIS), IgniteException.class, "The cluster should be inactive");
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreWithMissedPartitions() throws Exception {
        IgniteEx ignite = startGridsWithCache(4, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        forceCheckpoint();

        ignite.cluster().state(ClusterState.INACTIVE);

        stopGrid(2);
        stopGrid(3);

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(MAX_AWAIT_MILLIS), IgniteCheckedException.class, "not all partitions available");

        startGrid(2);
        startGrid(3);

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testBasicClusterSnapshotRestoreDiffTopology() throws Exception {
        int nodesCnt = 4;

        IgniteEx ignite = startGridsWithCache(nodesCnt - 2, dfltCacheCfg, CACHE_KEYS_RANGE);

        resetBaselineTopology();

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        startGrid(nodesCnt - 2);
        startGrid(nodesCnt - 1);

        resetBaselineTopology();
        awaitPartitionMapExchange();

        forceCheckpoint();

        ignite.cluster().state(ClusterState.INACTIVE);

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testBasicClusterSnapshotRestoreAfterDestroy() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        resetBaselineTopology();

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.destroyCache(dfltCacheCfg.getName());

        awaitPartitionMapExchange();

        forceCheckpoint();

        assertNull(ignite.cache(dfltCacheCfg.getName()));

        ignite.cluster().state(ClusterState.INACTIVE);

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        ignite.cluster().state(ClusterState.ACTIVE);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
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

    @Test
    public void testPreventRecoveryOnRestoredCacheGroup() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        resetBaselineTopology();

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        enableCheckpoints(G.allGrids(), false);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

//        forceCheckpoint();

        stopAllGrids();

        ignite = startGrid(0);
        startGrid(1);

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        stopAllGrids();

        ignite = startGrid(0);
        startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

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
