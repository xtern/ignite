/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collections;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.preload.GridPartitionBatchDemandMessage;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
public class GridCachePersistenceRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_PART_COUNT = 8;

    /** */
    private static final int TEST_SIZE = GridTestUtils.SF.applyLB(100_000, 10_000);

    /** */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

//    /** */
//    @After
//    public void setAfter() {
//        System.setProperty(IGNITE_PERSISTENCE_REBALANCE_ENABLED, "false");
//    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointFrequency(3_000) // todo check with default timeout!
                .setMaxWalArchiveSize(10 * 1024 * 1024 * 1024L))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.REPLICATED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, CACHE_PART_COUNT)));
//            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

//
//    /**
//     * @param ignite The ignite instance to check.
//     * @param check The mode to check.
//     * @return The number of pending switch requests.
//     */
//    private int getPendingSwitchRequests(IgniteEx ignite, CacheDataStoreEx.StorageMode check) {
//        return ignite.context()
//            .cache()
//            .context()
//            .preloadMgr()
//            .switcher()
//            .pendingRequests(new Predicate<CacheDataStoreEx.StorageMode>() {
//                @Override public boolean test(CacheDataStoreEx.StorageMode mode) {
//                    return mode == check;
//                }
//            });
//    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testPersistenceRebalanceBase() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        forceCheckpoint();

        IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        verifyLocalCacheContent(ignite0, ignite1, DEFAULT_CACHE_NAME);
    }

    private void verifyLocalCacheContent(IgniteEx node0, IgniteEx node1, String name) throws IgniteCheckedException {
        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        IgniteInternalCache<Integer, Integer> cache0 = node0.cachex(name);
        IgniteInternalCache<Integer, Integer> cache1 = node1.cachex(name);

        assertEquals(cache0.localSize(peekAll), cache1.localSize(peekAll));

        for (Cache.Entry<Integer, Integer> entry : cache0.localEntries(peekAll))
            assertEquals(entry.getValue(), cache1.localPeek(entry.getKey(), peekAll));
    }

    /** */
    @Test
    @Ignore
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceManualCache() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>("manual")
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setRebalanceDelay(-1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(8)));

        loadData(ignite0, "manual", TEST_SIZE);

        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());

        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());

        printPartitionState("manual", 0);

        cache.put(TEST_SIZE, new byte[1000]);

        awaitPartitionMapExchange(true, true, Collections.singleton(ignite1.localNode()), true);
    }

    /** */
    @Test
    @Ignore
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceAsyncUpdates() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(8)));

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());

        IgniteEx ignite1 = startGrid(1);

        TestRecordingCommunicationSpi.spi(ignite1)
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridPartitionBatchDemandMessage;
                }
            });

        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());

        TestRecordingCommunicationSpi.spi(ignite1).waitForBlocked();

        cache.put(TEST_SIZE, new byte[1000]);

        awaitPartitionMapExchange(true, true, Collections.singleton(ignite1.localNode()), true);
    }

    /**
     * @param ignite Ignite instance to load.
     * @param name The cache name to add random data to.
     * @param size The total size of entries.
     */
    protected void loadData(Ignite ignite, String name, int size) {
        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(name)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < size; i++) {
                if ((i + 1) % (size / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (size) + "% entries.");

                streamer.addData(i, i + name.hashCode());
            }
        }
    }
}
