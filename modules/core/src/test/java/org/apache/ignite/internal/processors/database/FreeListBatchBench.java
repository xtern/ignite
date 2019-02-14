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
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
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
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
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

    /** */
    private static final long DEF_REG_SIZE = 4 * 1024 * 1024 * 1024L;

    /** */
    private static final String REG_BATCH = "batch-region";

    /** */
    private static final String REG_SINGLE = "single-region";

    /** */
    private static final String CACHE_BATCH = "batch";

    /** */
    private static final String CACHE_SINGLE = "single";

    /** */
    private static final boolean MEM_STAT = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration reg1 = new DataRegionConfiguration();
        reg1.setInitialSize(DEF_REG_SIZE);
        reg1.setMaxSize(DEF_REG_SIZE);
        reg1.setMetricsEnabled(MEM_STAT);
        reg1.setName(REG_BATCH);

        DataRegionConfiguration reg2 = new DataRegionConfiguration();
        reg2.setInitialSize(DEF_REG_SIZE);
        reg2.setMaxSize(DEF_REG_SIZE);
        reg2.setMetricsEnabled(MEM_STAT);
        reg2.setName(REG_SINGLE);

        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        storeCfg.setDataRegionConfigurations(reg1, reg2);

        cfg.setDataStorageConfiguration(storeCfg);

        return cfg;
    }
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

        //bench(batchSize / 10, 50, 4096, 16384);
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
        long[] batchTimes = new long[subIters / 2];
        long[] singleTimes = new long[subIters / 2];

        IgniteEx node = grid(0);

        node.createCache(ccfg(true));
        node.createCache(ccfg(false));

        try {
            log.info(">>> Warm up " + subIters / 10 + " iterations.");

            int subOff  = subIters / 10;

            GridCacheContext cctxBatch = node.cachex(CACHE_BATCH).context();
            GridCacheContext cctxSingle = node.cachex(CACHE_SINGLE).context();

            for (int i = 0; i < subOff ; i++) {
                boolean batch = i % 2 == 0;

                doBatchUpdate(batch ? cctxBatch : cctxSingle, batch, batchSize, iterations, sizes, i * iterations);
            }

            log.info(">>> Starting " + subIters + " iterations, batch=" + batchSize);

            for (int i = 0; i < subIters; i++) {
                long batch, single;
                if (i % 2 == 0) {
                    batch = doBatchUpdate(cctxBatch, true, batchSize, iterations, sizes, i * iterations + (subOff * iterations));

                    batchTimes[i / 2] = batch;

                    batchTotalTime += batch;
                }
                 else {
                    single = doBatchUpdate(cctxSingle, false, batchSize, iterations, sizes, i * iterations + (subOff * iterations));

                    singleTimes[i / 2] = single;

                    singleTotalTime += single;
                }
            }

            // Check mean err.
            long batchAvg = batchTotalTime / (subIters / 2);
            long singleAvg = singleTotalTime / (subIters / 2);

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

            if (MEM_STAT) {
                IgniteCacheDatabaseSharedManager dbMgr = grid(0).context().cache().context().database();

                dbMgr.dumpStatistics(log());

                printMemMetrics(dbMgr, REG_BATCH);
                printMemMetrics(dbMgr, REG_SINGLE);
            }
        }
        finally {
            grid(0).destroyCache(CACHE_BATCH);
            grid(0).destroyCache(CACHE_SINGLE);
        }
    }

    /** */
    private long doBatchUpdate(
        GridCacheContext cctx,
        boolean batch,
        int batchSize,
        int iterations,
        int[] objSizes,
        int off
    ) throws Exception {
        GridDhtPreloader preloader = (GridDhtPreloader)cctx.group().preloader();

        GridDhtPartitionDemander demander = GridTestUtils.getFieldValue(preloader, "demander");

        long nanos = 0;

        for (int iter = off; iter < off + iterations; iter++) {
            List<GridCacheEntryInfo> infos = prepareBatch(cctx, iter * batchSize, batchSize, objSizes);

            long start = System.nanoTime();

            if (batch)
                demander.preloadEntries(null, 0, infos, cctx.topology().readyTopologyVersion());
            else
                demander.preloadEntriesSingle(null, 0, infos, cctx.topology().readyTopologyVersion());

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


    /** */
    private void printMemMetrics(IgniteCacheDatabaseSharedManager dbMgr, String regName) throws IgniteCheckedException {
        DataRegion reg = dbMgr.dataRegion(regName);

        DataRegionMetrics metrics = reg.memoryMetrics();

        log.info(regName + ": pages=" + metrics.getTotalAllocatedPages() +
            ", fill=" + new DecimalFormat("#0.0000").format(metrics.getPagesFillFactor()));
    }

    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg(boolean batch) {
        return new CacheConfiguration<K, V>(batch ? CACHE_BATCH : CACHE_SINGLE)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setDataRegionName(batch ? REG_BATCH : REG_SINGLE);
    }
}
