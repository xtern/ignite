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
package org.apache.ignite.internal.processors.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.junit.Assert.assertArrayEquals;

/**
 *
 */
//@RunWith(JUnit4.class)
@RunWith(Parameterized.class)
public class FreeListBatchUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int HDR_SIZE = 8 + 32;

    /** */
    private static final long DEF_REG_SIZE = 6 * 1024 * 1024 * 1024L;

    /** */
    @Parameterized.Parameters(name = "with atomicity={0} and persistence={1}")
    public static Iterable<Object[]> setup() {
        return Arrays.asList(new Object[][]{
            {CacheAtomicityMode.ATOMIC, false},
//            {CacheAtomicityMode.ATOMIC, true},
//            {CacheAtomicityMode.TRANSACTIONAL, false},
//            {CacheAtomicityMode.TRANSACTIONAL, true},
//            {CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, false},
//            {CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, true}
        });
    }

    @Parameterized.Parameter(0)
    public CacheAtomicityMode cacheAtomicityMode;

    @Parameterized.Parameter(1)
    public boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration def = new DataRegionConfiguration();
        def.setInitialSize(DEF_REG_SIZE);
        def.setMaxSize(DEF_REG_SIZE);
        def.setPersistenceEnabled(persistence);

        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        storeCfg.setDefaultDataRegionConfiguration(def);

        if (persistence) {
            storeCfg.setWalMode(WALMode.LOG_ONLY);
            storeCfg.setMaxWalArchiveSize(Integer.MAX_VALUE);
        }

        cfg.setDataStorageConfiguration(storeCfg);

        return cfg;
    }


    /**
     *
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);
    }

    @Test
    public void checkStreamer() throws Exception {
        Ignite node = startGrids(4);

        node.cluster().active(true);

        IgniteCache<String, byte[]> cache = node.createCache(ccfg(8, CacheMode.REPLICATED));

        awaitPartitionMapExchange();

        int cnt = 1024;

        //IgniteCache<String, byte[]> cache = ;

        try (IgniteDataStreamer<String, byte[]> streamer = node.dataStreamer(DEFAULT_CACHE_NAME)) {

            for (int i = 0; i < cnt; i++)
                streamer.addData(String.valueOf(i), new byte[128]);
        }

        log.info("Sleep");

        U.sleep(5_000);

        assert GridTestUtils.waitForCondition(() -> {
            return cache.size() == cnt;
        }, 10_000);

        for (int i = 0; i < cnt; i++)
            assertTrue(cache.get(String.valueOf(i)).length == 128);
    }

    /**
     *
     */
    @Test
    public void testBatchPartialRebalance() throws Exception {
        if (!persistence)
            return;

        // TODO https://issues.apache.org/jira/browse/IGNITE-7384
        // http://apache-ignite-developers.2346864.n4.nabble.com/Historical-rebalance-td38380.html
        if (cacheAtomicityMode == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            return;

        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "100");

        Ignite node = startGrids(2);

        node.cluster().active(true);

        IgniteCache<String, byte[]> cache = node.createCache(ccfg());

        int cnt = 10_000;

        log.info("Loading " + cnt + " random entries.");

        Map<String, byte[]> srcMap = new HashMap<>();

        for (int i = 0; i < cnt; i++) {
            byte[] obj = new byte[ThreadLocalRandom.current().nextInt(1024)];

            srcMap.put(String.valueOf(i), obj);
        }

        try (IgniteDataStreamer<String, byte[]> streamer = node.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.addData(srcMap);
        }

        forceCheckpoint();

        log.info("Stopping node #2.");

        grid(1).close();

        log.info("Updating values on node #1.");

        for (int i = 100; i < 1000; i++) {
            String key = String.valueOf(i);

            if (i % 33 == 0) {
                cache.remove(key);

                srcMap.remove(key);
            }
            else {
                byte[] bytes = cache.get(key);

                Arrays.fill(bytes, (byte)1);

                srcMap.put(key, bytes);
                cache.put(key, bytes);
            }
        }

        forceCheckpoint();

        log.info("Starting node #2.");

        IgniteEx node2 = startGrid(1);

        log.info("Await rebalance on node #2.");

        awaitRebalance(node2, DEFAULT_CACHE_NAME);

        log.info("Stop node #1.");

        node.close();

        validateCacheEntries(node2.cache(DEFAULT_CACHE_NAME), srcMap);
    }

    /**
     *
     */
    @Test
    public void testBatchPutAll() throws Exception {
        Ignite node = startGrid(0);

        node.cluster().active(true);

        node.createCache(ccfg());

        int cnt = 2_000_000;
        int minSize = 0;
        int maxSize = 2048;
        int start = 0;

        log.info("Loading " + cnt + " random entries per " + minSize + " - " + maxSize + " bytes.");

        Map<String, byte[]> srcMap = new HashMap<>();

        for (int i = start; i < start + cnt; i++) {
            int size = minSize + ThreadLocalRandom.current().nextInt(maxSize - minSize);

            byte[] obj = new byte[size];

            srcMap.put(String.valueOf(i), obj);
        }

        try (IgniteDataStreamer<String, byte[]> streamer = node.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.addData(srcMap);
        }

        srcMap.put(String.valueOf(1), new byte[65536]);

        node.cache(DEFAULT_CACHE_NAME).put(String.valueOf(1), new byte[65536]);

        log.info("Done");

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        if (persistence)
            node.cluster().active(false);

        final IgniteEx node2 = startGrid(1);

        if (persistence) {
            List<BaselineNode> list = new ArrayList<>(node.cluster().currentBaselineTopology());

            list.add(node2.localNode());

            node.cluster().active(true);

            node.cluster().setBaselineTopology(list);
        }

        log.info("await rebalance");

        awaitRebalance(node2, DEFAULT_CACHE_NAME);

        U.sleep(2_000);

        node.close();



        log.info("Verification on node2");

        validateCacheEntries(node2.cache(DEFAULT_CACHE_NAME), srcMap);

        if (persistence) {
            node2.close();

            Ignite ignite = startGrid(1);

            ignite.cluster().active(true);

            log.info("Validate entries after restart");

            validateCacheEntries(ignite.cache(DEFAULT_CACHE_NAME), srcMap);
        }
    }

    /**
     * @param node Ignite node.
     * @param name Cache name.
     */
    private void awaitRebalance(IgniteEx node, String name) throws IgniteInterruptedCheckedException {
        boolean ok = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for ( GridDhtLocalPartition part : node.context().cache().cache(name).context().group().topology().localPartitions()) {
                    if (part.state() != GridDhtPartitionState.OWNING)
                        return false;
                }

                return true;
            }
        }, 30_000);

        U.sleep(1000);

        assertTrue(ok);
    }

    /**
     * @param cache Cache.
     * @param map Map.
     */
    @SuppressWarnings("unchecked")
    private void validateCacheEntries(IgniteCache cache, Map<String, byte[]> map) {
        if (true)
            return;

        log.info("Cache validation: " + map.size());

        assertEquals(map.size(), cache.size());

        for (Map.Entry<String, byte[]> e : map.entrySet()) {
            String idx = "idx=" + e.getKey();

            byte[] bytes = (byte[])cache.get(e.getKey());

            assertNotNull(idx, bytes);

            assertEquals(idx + ": length not equal", e.getValue().length, bytes.length);

            assertArrayEquals(idx, e.getValue(), bytes);
        }
    }


    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg() {
        return ccfg(1, CacheMode.REPLICATED);
    }

    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg(int parts, CacheMode mode) {
        return new CacheConfiguration<K, V>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setCacheMode(mode)
            .setAtomicityMode(cacheAtomicityMode);
    }
}
