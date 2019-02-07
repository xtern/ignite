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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class FreeListBatchBench extends GridCommonAbstractTest {
    /** */
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#0.0");

    /**
     *
     */
    @Test
    public void testBatch() throws Exception {
        startGrid(0);

        int batchSize = 500;

        bench(batchSize, 50, 0, 4);
        bench(batchSize, 50, 0, 16);
        bench(batchSize, 50, 0, 256);
        bench(batchSize, 50, 0, 512);
        bench(batchSize, 50, 0, 1024);
        bench(batchSize, 20, 0, 8192);
        bench(batchSize, 10, 4096, 16384);
        bench(batchSize / 10, 100, 4096, 16384);
        bench(batchSize / 50, 500, 4096, 16384);
        bench(batchSize / 100, 1000, 4096, 16384);
//        doBatch(2, 1000, 4096, 16384);
    }

    /** */
    private void bench(int batchSize, int iterations, int minObjSIze, int maxObjSize) throws Exception {
        int sizes[] = new int[batchSize];
        int minSize = maxObjSize;
        int maxSize = minObjSIze;
        long sum = 0;

        for (int i = 0; i < batchSize; i++) {
            int size = sizes[i] = minObjSIze + ThreadLocalRandom.current().nextInt(maxObjSize - minObjSIze);

            if (size < minSize)
                minSize = size;

            if (size > maxSize)
                maxSize = size;

            sum += size;
        }

        long avgSize = sum / sizes.length;

        int subIters = 100;

        long batchTotalTime = 0;
        long singleTotalTime = 0;
        long[] batchTimes = new long[subIters];
        long[] singleTimes = new long[subIters];

        IgniteEx node = grid(0);

        node.createCache(ccfg());

        try {
            GridCacheContext cctx = node.cachex(DEFAULT_CACHE_NAME).context();

            log.info(">>> Warm up " + subIters / 10 + " iterations.");

            for (int i = 0; i < subIters / 10; i++)
                doBatchUpdate(cctx, i % 2 == 0, batchSize, iterations, sizes);

            log.info(">>> Starting " + subIters + " iterations, batch=" + batchSize);

            for (int i = 0; i < subIters; i++) {
                long batch = doBatchUpdate(cctx,true, batchSize, iterations, sizes);
                long single = doBatchUpdate(cctx,false, batchSize, iterations, sizes);

                batchTimes[i] = batch;
                singleTimes[i] = single;

                batchTotalTime += batch;
                singleTotalTime += single;
            }

            // Check mean err.
            long batchAvg = batchTotalTime / subIters;
            long singleAvg = singleTotalTime / subIters;

            double batchMean = meanError(batchTimes, batchAvg);
            double singleMean = meanError(singleTimes, singleAvg);

            String str = String.format("\n####################################################################################\n" +
                    "\t>>> cnt=%d\n" +
                    "\t>>> objects size: min=%d, max=%d, avg=%d\n" +
                    "\t>>> time: batch=%d, single=%d  ---> %s%%\n" +
                    "######[MEANS]#######################################################################\n" +
                    "\t>>>  Batch: %.4f (avg=%d) %s%%\n" +
                    "\t>>> Single: %.4f (avg=%d) %s%%" +
                    "\n####################################################################################",
                batchSize, minSize, maxSize, avgSize, batchAvg, singleAvg, percent(batchAvg, singleAvg),
                batchMean, batchAvg, DECIMAL_FORMAT.format((batchMean / (double)batchAvg) * 100),
                singleMean, singleAvg, DECIMAL_FORMAT.format((singleMean / (double)singleAvg) * 100));

            log.info(str);
        }
        finally {
            grid(0).destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /** */
    private long doBatchUpdate(
        GridCacheContext cctx,
        boolean batch,
        int batchSize,
        int iterations,
        int[] objSizes
    ) throws Exception {
        GridDhtPreloader preloader = (GridDhtPreloader)cctx.group().preloader();

        GridDhtPartitionDemander demander = GridTestUtils.getFieldValue(preloader, "demander");

        long nanos = 0;

        for (int iter = 0; iter < iterations; iter++) {
            List<GridCacheEntryInfo> infos = prepareBatch(cctx, iter * batchSize, batchSize, objSizes);

            long start = System.nanoTime();

            if (batch)
                demander.preloadEntries(null, 0, infos, cctx.topology().readyTopologyVersion());
            else
                demander.preloadEntries2(null, 0, infos, cctx.topology().readyTopologyVersion());

            nanos += (System.nanoTime() - start);
        }

        return nanos / iterations;
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

    /**
     * Generates rebalance info objects.
     *
     * @param cctx Cache context.
     * @param off Offset.
     * @param cnt Count.
     * @param sizes Object sizes.
     * @return List of generated objects.
     */
    private List<GridCacheEntryInfo> prepareBatch(GridCacheContext cctx, int off, int cnt, int[] sizes) {
        List<GridCacheEntryInfo> infos = new ArrayList<>();

        //GridCacheVersion ver = new GridCacheVersion((int)cctx.topology().readyTopologyVersion().topologyVersion(), 0, 0, 0);

        for (int i = off; i < off + cnt; i++) {
            int size = sizes[i - off];

            KeyCacheObject key = cctx.toCacheKeyObject(String.valueOf(i));
            CacheObject val = cctx.toCacheObject(new byte[size]);

            GridCacheEntryInfo info = new GridCacheEntryInfo();
            info.key(key);
            info.value(val);
            info.cacheId(cctx.cacheId());
            info.version(cctx.shared().versions().startVersion());

            infos.add(info);
        }

        return infos;
    }

    /**
     * Format percentage.
     */
    private String percent(long time, long time1) {
        return DECIMAL_FORMAT.format((100 - ((double)time) / ((double)time1) * 100) * -1);
    }

    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg() {
        return new CacheConfiguration<K, V>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);
    }
}
