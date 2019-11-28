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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
//import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
@RunWith(Parameterized.class)
public class GridCacheFileRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int TEST_SIZE = GridTestUtils.SF.applyLB(100_000, 10_000);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    @Parameterized.Parameter
    public CacheAtomicityMode cacheAtomicityMode;

    private CacheMode cacheMode = REPLICATED;

    private int parts = 16;

    private int backups = 0;

    private CacheWriteSynchronizationMode cacheWriteSyncMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;

    /** */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** Parameters. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        List<Object[]> params = new ArrayList<>(2);

        params.add(new CacheAtomicityMode[] {CacheAtomicityMode.TRANSACTIONAL});
//        params.add(new CacheAtomicityMode[] {CacheAtomicityMode.ATOMIC});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000;
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return 60_000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(10 * 1024 * 1024L)
                    .setMaxSize(4 * 1024L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setDataRegionConfigurations(new DataRegionConfiguration()
                    .setInitialSize(10 * 1024 * 1024L)
                    .setMaxSize(4 * 1024 * 1024 * 1024L)
                    .setPersistenceEnabled(true)
                    .setName("someRegion"))
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointFrequency(3_000)) // todo check with default timeout!
//                .setWalSegmentSize(4 * 1024 * 1024)
//                .setMaxWalArchiveSize(32 * 1024 * 1024 * 1024L))
            .setCacheConfiguration(cacheConfig(DEFAULT_CACHE_NAME).setDataRegionName("someRegion"), cacheConfig(CACHE1), cacheConfig(CACHE2));

        cfg.setSystemThreadPoolSize(56);
        //.setCacheConfiguration(cacheConfig(CACHE1));

//        if (getTestIgniteInstanceIndex(igniteInstanceName) == 2)
//            cfg.setGridLogger(new NullLogger());

        return cfg;
    }

    private CacheConfiguration cacheConfig(String name) {
        return new CacheConfiguration(name).setCacheMode(cacheMode)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setAtomicityMode(cacheAtomicityMode)
            .setWriteSynchronizationMode(cacheWriteSyncMode)
            //.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
//                .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(backups);
//            .setCommunicationSpi(new TestRecordingCommunicationSpi()
    }

//    @Test
//    public void testEvictReadOnlyPartition() {
//
//    }

    /** todo */
    @Test
//    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value="0")
    public void testMultipleCachesCancelRebalancePartitionedUnderConstantLoadUnstableTopology() throws Exception {
        cacheMode = PARTITIONED;
        backups = 3;

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        int timeout = 180_000;

        try {
            IgniteEx ignite0 = startGrid(0);

            ignite0.cluster().active(true);

            blt.add(ignite0.localNode());

            ignite0.cluster().setBaselineTopology(blt);

            loadData(ignite0, CACHE1, entriesCnt);
            loadData(ignite0, CACHE2, entriesCnt);

            forceCheckpoint(ignite0);

            AtomicLong cntr = new AtomicLong(entriesCnt);

            ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

            IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "loader");

            long endTime = System.currentTimeMillis() + timeout;

            int nodes = 3;

            int started = 1;

            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(1000);

                IgniteEx igniteX = startGrid(i + started);

                blt.add(igniteX.localNode());

                if (time0 % 2 == 0)
                    U.sleep(time0);

                ignite0.cluster().setBaselineTopology(blt);
            }

            forceCheckpoint();

            do {
                for (int i = 0; i < nodes; i++) {
                    int time0 = ThreadLocalRandom.current().nextInt(2000);

                    U.sleep(time0);

                    stopGrid(i + started);
                }

                awaitPartitionMapExchange();

                for (int i = 0; i < nodes; i++) {
                    int time0 = ThreadLocalRandom.current().nextInt(1000);

                    if (time0 % 2 == 0)
                        U.sleep(time0);

                    startGrid(i + started);

                    //                blt.add(igniteX.localNode());;



                    //                ignite0.cluster().setBaselineTopology(blt);
                }

                awaitPartitionMapExchange();
            }
            while (U.currentTimeMillis() < endTime);

            awaitPartitionMapExchange();

            ldr.stop();

            ldrFut.get();

            for (Ignite g : G.allGrids()) {
                verifyCacheContent(g.cache(CACHE1), ldr.cntr.get());
                verifyCacheContent(g.cache(CACHE2), entriesCnt);
            }
        } catch (Error | RuntimeException | IgniteCheckedException e) {
//            for (Ignite g : G.allGrids()) {
//                GridPartitionFilePreloader filePreloader = ((IgniteEx)g).context().cache().context().filePreloader();
//
//                synchronized (System.err) {
//                    if (filePreloader != null)
//                        filePreloader.printDiagnostic();
//                }
//            }

            throw e;
        }
    }

    @Test
//    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    //@WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalancePartitionedUnderConstantLoad2() throws Exception {
        cacheMode = PARTITIONED;
        backups = 3;

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        int timeout = 180_000;

        try {
            IgniteEx ignite0 = startGrid(0);

            ignite0.cluster().active(true);

            blt.add(ignite0.localNode());

            ignite0.cluster().setBaselineTopology(blt);

            loadData(ignite0, CACHE1, entriesCnt);
            loadData(ignite0, CACHE2, entriesCnt);

            forceCheckpoint(ignite0);

            AtomicLong cntr = new AtomicLong(entriesCnt);

            ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

            IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "loader");

            long endTime = System.currentTimeMillis() + timeout;

            int nodes = 3;

            int started = 1;

            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(1000);

                IgniteEx igniteX = startGrid(i + started);

                blt.add(igniteX.localNode());

                if (time0 % 2 == 0)
                    U.sleep(time0);

                ignite0.cluster().setBaselineTopology(blt);
            }

            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(2000);

                U.sleep(time0);

                stopGrid(i + started);
            }

            U.sleep(3_000);


            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(1000);

                if (time0 % 2 == 0)
                    U.sleep(time0);

                System.out.println("*******************************");
                System.out.println("  starting test killer " + (i + started));
                System.out.println("*******************************");

                startGrid(i + started);
            }


//            do {

//
//                awaitPartitionMapExchange();
//
//                for (int i = 0; i < nodes; i++) {
//                    int time0 = ThreadLocalRandom.current().nextInt(1000);
//
//                    if (time0 % 2 == 0)
//                        U.sleep(time0);
//
//                    startGrid(i + started);
//
//                    //                blt.add(igniteX.localNode());;
//
//
//
//                    //                ignite0.cluster().setBaselineTopology(blt);
//                }
//
//                awaitPartitionMapExchange();
//            }
//            while (U.currentTimeMillis() < endTime);

            awaitPartitionMapExchange();

            ldr.stop();

            ldrFut.get();

            for (Ignite g : G.allGrids()) {
                verifyCacheContent(g.cache(CACHE1), ldr.cntr.get());
                verifyCacheContent(g.cache(CACHE2), entriesCnt);
            }
        } catch (Error | RuntimeException | IgniteCheckedException e) {
//            for (Ignite g : G.allGrids()) {
//                GridPartitionFilePreloader filePreloader = ((IgniteEx)g).context().cache().context().filePreloader();
//
//                synchronized (System.err) {
//                    if (filePreloader != null)
//                        filePreloader.printDiagnostic();
//                }
//            }

            throw e;
        }
    }

    private void verifyCacheContent(IgniteCache<Object, Object> cache, long cnt) {
        verifyCacheContent(cache, cnt, false);
    }

    // todo should check partitions
    private void verifyCacheContent(IgniteCache<Object, Object> cache, long cnt, boolean removes) {
        log.info("Verifying cache contents [cache=" + cache.getName() + ", size=" + cnt + "]");

        StringBuilder buf = new StringBuilder();

        int fails = 0;

        long expSize = 0;

        for (long k = 0; k < cnt; k++) {
            if (removes && k % 10 == 0)
                continue;

            ++expSize;

            Long exp = generateValue(k, cache.getName());;
            Long actual = (Long)cache.get(k);

            if (!Objects.equals(exp, actual)) {
//                if (fails++ < 100)
                buf.append("cache=").append(cache.getName()).append(", key=").append(k).append(", expect=").append(exp).append(", actual=").append(actual).append('\n');
//                else {
//                    buf.append("\n... and so on\n");

//                    break;
//                }
            }

            if ((k + 1) % (cnt / 10) == 0)
                log.info("Verification: " + (k + 1) * 100 / cnt + "%");
        }

        if (!removes && cnt != cache.size())
            buf.append("\ncache=").append(cache.getName()).append(" size mismatch [expect=").append(cnt).append(", actual=").append(cache.size()).append('\n');

        assertTrue(buf.toString(), buf.length() == 0);
    }

    /**
     * @param ignite Ignite instance to load.
     * @param name The cache name to add random data to.
     * @param size The total size of entries.
     */
    private void loadData(Ignite ignite, String name, int size) {
        try (IgniteDataStreamer<Long, Long> streamer = ignite.dataStreamer(name)) {
            streamer.allowOverwrite(true);

            for (long i = 0; i < size; i++) {
                if ((i + 1) % (size / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (size) + "% entries.");

                streamer.addData(i, generateValue(i, name));
            }
        }
    }

    /**
     * @param expCache Expected data cache.
     * @param actCache Actual data cache.

     * @throws IgniteCheckedException If failed.
     */
    private void verifyLocalCache(IgniteInternalCache<Integer, Integer> expCache,
        IgniteInternalCache<Integer, Integer> actCache) throws IgniteCheckedException {
        StringBuilder buf = new StringBuilder();

        buf.append(verifyLocalCacheContent(expCache, actCache));
        buf.append(verifyLocalCacheContent(actCache, expCache));

        for (GridDhtLocalPartition actPart : expCache.context().topology().currentLocalPartitions()) {
            GridDhtLocalPartition expPart = actCache.context().topology().localPartition(actPart.id());

            if (actPart.state() != expPart.state())
                buf.append("\n").append(expCache.context().localNodeId()).append(" vs ").append(actCache.context().localNodeId()).append(" state mismatch p=").append(actPart.id()).append(" exp=").append(expPart.state()).append(" act=").append(actPart.state());

            long expCntr = expPart.updateCounter();
            long actCntr = actPart.updateCounter();

            if (expCntr != actCntr)
                buf.append("\n").append("Counter not match p=").append(expPart.id()).append(", exp=").append(expCntr).append(", act=").append(actCntr);

            long expSize = expPart.fullSize();
            long actSize = actPart.fullSize();

            if (expSize != actSize)
                buf.append("\n").append("Size not match p=").append(expPart.id()).append(", exp=").append(expSize).append(", act=").append(actSize);
        }

        assertTrue(buf.toString(), buf.length() == 0);
    }

    /**
     * @param cache1 Expected data cache.
     * @param cache2 Actual data cache.

     * @throws IgniteCheckedException If failed.
     * @return Buffer with descriptions of found problems during verification.
     */
    private StringBuilder verifyLocalCacheContent(IgniteInternalCache<Integer, Integer> cache1,
        IgniteInternalCache<Integer, Integer> cache2) throws IgniteCheckedException {

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        UUID node1 = cache1.context().shared().localNodeId();
        UUID node2 = cache2.context().shared().localNodeId();

        StringBuilder buf = new StringBuilder();

        for (Cache.Entry<Integer, Integer> entry : cache1.localEntries(peekAll)) {
            Object expect = entry.getValue();
            Object actual = cache2.localPeek(entry.getKey(), peekAll);

            if (!Objects.equals(expect, actual))
                buf.append("\n").append(node1).append(" vs ").append(node2).append(", key=").append(entry.getKey()).append(", expected=").append(expect).append(", actual=").append(actual);

            if (buf.length() > 10 * 1024) {
                buf.append("\n").append("... and so on");

                break;
            }
        }

        return buf;
    }

    private static long generateValue(long num, String str) {
        return num + str.hashCode();
    }

    /** */
    private static class ConstantLoader implements Runnable {
        /** */
        private final AtomicLong cntr;

        /** */
        private final boolean enableRemove;

        /** */
        private final CyclicBarrier pauseBarrier;

        /** */
        private volatile boolean pause;

        /** */
        private volatile boolean paused;

        /** */
        private volatile boolean stop;

        /** */
        private final IgniteCache<Long, Long> cache;

        /** */
        public ConstantLoader(IgniteCache<Long, Long> cache, AtomicLong cntr, boolean enableRemove, int threadCnt) {
            this.cache = cache;
            this.cntr = cntr;
            this.enableRemove = enableRemove;
            this.pauseBarrier = new CyclicBarrier(threadCnt + 1); // +1 waiter
        }

        /** {@inheritDoc} */
        @Override public void run() {
            String cacheName = cache.getName();

            while (!stop && !Thread.currentThread().isInterrupted()) {
                if (pause) {
                    if (!paused) {
                        U.awaitQuiet(pauseBarrier);

                        log.info("Async loader paused.");

                        paused = true;
                    }

                    // Busy wait for resume.
                    try {
                        U.sleep(100);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        break;
                    }

                    continue;
                }

                long from = cntr.getAndAdd(100);

                for (long i = from; i < from + 100; i++)
                    cache.put(i, generateValue(i, cacheName));

                if (!enableRemove)
                    continue;

                for (long i = from; i < from + 100; i += 10)
                    cache.remove(i);
            }

            log.info("Async loader stopped.");
        }

        /**
         * Stop loader thread.
         */
        public void stop() {
            stop = true;
        }

        /**
         * Pause loading.
         */
        public void pause() {
            pause = true;

            log.info("Suspending loader threads: " + pauseBarrier.getParties());

            // Wait all workers came to barrier.
            U.awaitQuiet(pauseBarrier);

            log.info("Loader suspended");
        }

        /**
         * Resume loading.
         */
        public void resume() {
            paused = false;
            pause = false;

        }
    }
}
