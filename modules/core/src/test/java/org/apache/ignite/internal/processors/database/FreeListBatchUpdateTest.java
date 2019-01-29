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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.CIX3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class FreeListBatchUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int HDR_SIZE = 8 + 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(6*1024*1024*1024L)));

        //cfg.setCacheConfiguration();

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
     *
     */
    @Test
    public void testBatchPutAll() throws Exception {
        Ignite node = startGrid(0);

        node.createCache(ccfg());

        int cnt = 200_000;
        int minSize = 0;
        int maxSize = 16384;
        int start = 0;

        log.info("Loading " + cnt + " random entries per " + minSize + " - " + maxSize + " bytes.");

        Map<Integer, byte[]> srcMap = new HashMap<>();

        for (int i = start; i < start + cnt; i++) {
            int size = minSize + ThreadLocalRandom.current().nextInt(maxSize - minSize);

            byte[] obj = new byte[size];

            srcMap.put(i, obj);
        }

        try (IgniteDataStreamer<Integer, byte[]> streamer = node.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.addData(srcMap);
        }

        log.info("Done");

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        validateCacheEntries(cache, srcMap);

        final IgniteEx node2 = startGrid(1);

        log.info("await rebalance");

        boolean ok = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for ( GridDhtLocalPartition part : node2.context().cache().cache(DEFAULT_CACHE_NAME).context().group().topology().localPartitions()) {
                    if (part.state() != GridDhtPartitionState.OWNING)
                        return false;
                }

                return true;
            }
        }, 10_000);

        assertTrue(ok);

        node.close();

        U.sleep(2_000);

        log.info("Verification on node2");

        validateCacheEntries(node2.cache(DEFAULT_CACHE_NAME), srcMap);
    }

    /**
     * @param cache Cache.
     * @param map Map.
     */
    @SuppressWarnings("unchecked")
    private void validateCacheEntries(IgniteCache cache, Map<Integer, byte[]> map) {
        assertEquals(map.size(), cache.size());

        for (Map.Entry<Integer, byte[]> e : map.entrySet()) {
            String idx = "idx=" + e.getKey();

            byte[] bytes = (byte[])cache.get(e.getKey());

            assertNotNull(idx, bytes);

            assertEquals(idx + ": length not equal", e.getValue().length, bytes.length);

            assertTrue(Arrays.equals(e.getValue(), bytes));
        }
    }

    /**
     *
     */
    @Test
    public void testBatch() throws Exception {
        startGrid(0);

        int batchSize = 500;

        doBatch(batchSize, 50, 0, 4);
        doBatch(batchSize, 50, 0, 16);
        doBatch(batchSize, 50, 0, 256);
        doBatch(batchSize, 50, 0, 512);
        doBatch(batchSize, 50, 0, 1024);
        doBatch(batchSize, 20, 0, 8192);
        doBatch(batchSize, 10, 4096, 16384);
        doBatch(batchSize / 10, 100, 4096, 16384);
        doBatch(batchSize / 50, 500, 4096, 16384);
        doBatch(batchSize / 100, 1000, 4096, 16384);
//        doBatch(2, 1000, 4096, 16384);
    }

    private void doBatch(int batchSize, int iterations, int minObjSIze, int maxObjSize) throws Exception {
        int sizes[] = generateSizes(batchSize, minObjSIze, maxObjSize);

        int min = maxObjSize, max = minObjSIze;
        long sum = 0;

        for (int s : sizes) {
            if (s < min)
                min = s;

            if (s > max)
                max = s;

            sum += s;
        }

//        int warmUp = iterations / 5;

        grid(0).createCache(ccfg());

        for (int i = 0; i < 4; i++) {
            log.info("warm up #" + i);

            doTestBatch(i % 2 == 0, batchSize, iterations, sizes);
        }

        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        int subIters = 100;

        long[][] times = new long[2][subIters];

        long total0 = 0, total1 = 0;

        DecimalFormat fmt = new DecimalFormat("#0.0");

        log.info("Checking");

        grid(0).createCache(ccfg());

        try {
            for (int i = 0; i < 5; i++) {
                doTestBatch(false, batchSize, iterations, sizes);
                doTestBatch(true, batchSize, iterations, sizes);
            }
            for (int i = 0; i < subIters; i++) {
                long time0 = doTestBatch(false, batchSize, iterations, sizes);
                long time1 = doTestBatch(true, batchSize, iterations, sizes);

                times[0][i] = time0;
                times[1][i] = time1;

                total0 += time0;
                total1 += time1;
            }

            // Check mean err.
            long avg0 = total0 / subIters;
            long avg1 = total1 / subIters;

            double mean0 = meanError(times[0], avg0);
            double mean1 = meanError(times[1], avg1);

            //        log.info("single=" + single + ", avg=" + avg + ", err=" + new DecimalFormat("#0.0").format(mean) + ": " +
            //            new DecimalFormat("#0.0").format((mean / (double)avg) * 100) + "%");

            String str = String.format("\n####################################################################################\n" +
                    "\t>>> cnt=%d\n" +
                    "\t>>> objects size: min=%d, max=%d, avg=%d\n" +
                    "\t>>> time: batch=%d, single=%d  ---> %s%%\n" +
                    "######[MEANS]#######################################################################\n" +
                    "\t>>>  Batch: %.4f (avg=%d) %s%%\n" +
                    "\t>>> Single: %.4f (avg=%d) %s%%"+
                    "\n####################################################################################",
                batchSize, min, max, sum / sizes.length, avg0, avg1, percent(avg0, avg1),
                mean0, avg0, fmt.format((mean0 / (double)avg0) * 100),
                mean1, avg1, fmt.format((mean1 / (double)avg1) * 100));

            log.info(str);
        } finally {
            grid(0).destroyCache(DEFAULT_CACHE_NAME);
        }
    }


    private long doTestBatch(boolean single, int batchSize, int iterations, int[] objSizes) throws Exception {
        IgniteEx node = grid(0);

        IgniteInternalCache cache = node.cachex(DEFAULT_CACHE_NAME);

        GridCacheContext cctx = cache.context();

        GridDhtPreloader preloader = (GridDhtPreloader)cctx.group().preloader();

        GridDhtPartitionDemander demander = GridTestUtils.getFieldValue(preloader, "demander");

        long nanos = 0;

        for (int iter = 0; iter < iterations; iter++) {
            List<GridCacheEntryInfo> infos = prepareBatch(cctx, iter * batchSize, batchSize, objSizes);

            long start = System.nanoTime();

            if (single)
                demander.preloadEntries1(null, 0, infos, cctx.topology().readyTopologyVersion());
            else
                demander.preloadEntries(null, 0, infos, cctx.topology().readyTopologyVersion());

            nanos += (System.nanoTime() - start);
        }

        long avg = nanos / iterations;

//        node.destroyCache(DEFAULT_CACHE_NAME);

        return avg;
    }

    /**
     * @return Mean squared error.
     */
    public double meanError(long[] times, long avg) {
        double sum = 0.0;

        for (int i = 0; i < times.length; i++) {
            double x = (double)(times[i] - avg);

            sum += x * x;
        }

        return Math.sqrt(sum / (times.length - 1));
    }

    private List<GridCacheEntryInfo> prepareBatch(GridCacheContext cctx, int off, int cnt, int[] sizes) {
        List<GridCacheEntryInfo> infos = new ArrayList<>();

        GridCacheVersion ver = new GridCacheVersion((int)cctx.topology().readyTopologyVersion().topologyVersion(), 0, 0, 0);

        for (int i = off; i < off + cnt; i++) {
            int size = sizes[i - off];

            KeyCacheObject key = cctx.toCacheKeyObject(i);
            CacheObject val = cctx.toCacheObject(new byte[size]);

            GridCacheEntryInfo info = new GridCacheEntryInfo();
            info.key(key);
            info.value(val);
            info.cacheId(cctx.cacheId());
            info.version(ver);

            infos.add(info);
        }

        return infos;
    }

    /**
     * @param cnt Items count.
     * @param minSize Minimum object size.
     * @param maxSize Maximum object size.
     * @return Array of random integers.
     */
    private int[] generateSizes(int cnt, int minSize, int maxSize) {
        int sizes[] = new int[cnt];

        for (int i = 0; i < cnt; i++)
            sizes[i] = minSize + ThreadLocalRandom.current().nextInt(maxSize - minSize);

        return sizes;
    }

    /**
     * Format percentage.
     */
    private String percent(long time, long time1) {
        return new DecimalFormat("#0.00").format((100 - ((double)time) / ((double)time1) * 100) * -1);
    }

    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg() {
        return new CacheConfiguration<K, V>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setCacheMode(CacheMode.REPLICATED);
    }
}
