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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_STORAGE_BATCH_PAGE_WRITE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/**
 *
 */
@RunWith(Parameterized.class)
public class FreeListPreloadWithBatchUpdatesTest extends GridCommonAbstractTest {
    /** */
    private static final long DEF_REG_SIZE_INIT = 3400 * 1024 * 1024L;

    /** */
    private static final long DEF_REG_SIZE = 6144 * 1024 * 1024L;

    /** */
    private static final int LARGE_PAGE = 16 * 1024;

    /** */
    private static final String DEF_CACHE_NAME = "some-cache";

    /** */
    @Parameterized.Parameters(name = " atomicity={0}, persistence={1}, pageSize={2}")
    public static Iterable<Object[]> setup() {
        return Arrays.asList(new Object[][]{
            {CacheAtomicityMode.ATOMIC, false, DFLT_PAGE_SIZE},
            {CacheAtomicityMode.ATOMIC, true, DFLT_PAGE_SIZE},
            {CacheAtomicityMode.ATOMIC, false, LARGE_PAGE},
            {CacheAtomicityMode.ATOMIC, true, LARGE_PAGE},
            {CacheAtomicityMode.TRANSACTIONAL, false, DFLT_PAGE_SIZE},
            {CacheAtomicityMode.TRANSACTIONAL, true, DFLT_PAGE_SIZE},
            {CacheAtomicityMode.TRANSACTIONAL, false, LARGE_PAGE},
            {CacheAtomicityMode.TRANSACTIONAL, true, LARGE_PAGE},
            {CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, false, DFLT_PAGE_SIZE},
            {CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, true, DFLT_PAGE_SIZE}
        });
    }

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode cacheAtomicityMode;

    /** */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** */
    @Parameterized.Parameter(2)
    public Integer pageSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration def = new DataRegionConfiguration();
        def.setInitialSize(DEF_REG_SIZE_INIT);
        def.setMaxSize(DEF_REG_SIZE);
        def.setPersistenceEnabled(persistence);

        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        storeCfg.setDefaultDataRegionConfiguration(def);
        storeCfg.setPageSize(pageSize);

        if (persistence) {
            storeCfg.setWalMode(WALMode.LOG_ONLY);
            storeCfg.setMaxWalArchiveSize(Integer.MAX_VALUE);
        }

        cfg.setDataStorageConfiguration(storeCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEF_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(cacheAtomicityMode));

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
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DATA_STORAGE_BATCH_PAGE_WRITE, value = "true")
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "100")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testBatchRebalance() throws Exception {
        Ignite node1 = startGrid(0);

        node1.cluster().active(true);

        int cnt = 50_000;

        int minSize = 0;
        int maxSize = 16384;

        Map<Integer, byte[]> srcMap = new HashMap<>();

        for (int i = 0; i < cnt; i++) {
            int size = maxSize == minSize ? maxSize : minSize + ThreadLocalRandom.current().nextInt(maxSize - minSize);

            byte[] obj = new byte[size];

            srcMap.put(i, obj);
        }

        try (IgniteDataStreamer<Integer, byte[]> streamer = node1.dataStreamer(DEF_CACHE_NAME)) {
            streamer.addData(srcMap);
        }

        log.info("Data loaded.");

        if (persistence)
            node1.cluster().active(false);

        final IgniteEx node2 = startGrid(1);

        if (persistence) {
            node1.cluster().active(true);

            node1.cluster().setBaselineTopology(node1.cluster().forServers().nodes());
        }

        log.info("Await rebalance.");

        awaitPartitionMapExchange();

        if (persistence)
            forceCheckpoint(node1);

        node1.close();

        IgniteCache<Object, Object> cache = node2.cache(DEF_CACHE_NAME);

        validateCacheEntries(cache, srcMap);

        // Check WAL rebalance.
        if (persistence) {
            log.info("Updating values on node #1.");

            // Update existing extries.
            for (int i = 100; i < 5_000; i++) {
                if (i % 3 == 0) {
                    cache.remove(i);

                    srcMap.remove(i);
                }
                else {
                    byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(maxSize)];

                    Arrays.fill(bytes, (byte)1);

                    srcMap.put(i, bytes);
                    cache.put(i, bytes);
                }
            }

            // New entries.
            for (int i = cnt; i < cnt + 1_000; i++) {
                byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(maxSize)];

                srcMap.put(i, bytes);
                cache.put(i, bytes);
            }

            forceCheckpoint(node2);

            log.info("Starting node #1");

            node1 = startGrid(0);

            node1.cluster().active(true);

            node1.cluster().setBaselineTopology(node1.cluster().forServers().nodes());

            log.info("Await rebalance on node #1.");

            awaitPartitionMapExchange();

            log.info("Stop node #2.");

            node2.close();

            validateCacheEntries(node1.cache(DEF_CACHE_NAME), srcMap);
        }
    }

    /**
     * @param cache Cache.
     * @param map Map.
     */
    @SuppressWarnings("unchecked")
    private void validateCacheEntries(IgniteCache cache, Map<?, byte[]> map) {
        int size = cache.size();

        assertEquals("Cache size mismatch.", map.size(), size);

        log.info("Validation " + cache.getName() + ", size=" + size);

        for (Map.Entry<?, byte[]> e : map.entrySet()) {
            String idx = "key=" + e.getKey();

            assertEquals(idx, e.getValue().length, ((byte[])cache.get(e.getKey())).length);
        }
    }
}
