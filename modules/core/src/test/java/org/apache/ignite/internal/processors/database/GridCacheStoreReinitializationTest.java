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

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridCachePreloadSharedManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.io.GridFileUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.util.IgniteUtils.GB;

/**
 *
 */
public class GridCacheStoreReinitializationTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 4;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC); // todo other sync modes

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dscfg = new DataStorageConfiguration();

        dscfg.setWalMode(WALMode.LOG_ONLY);

        DataRegionConfiguration reg = new DataRegionConfiguration();

        reg.setMaxSize(2 * GB);
        reg.setPersistenceEnabled(true);

        dscfg.setDefaultDataRegionConfiguration(reg);
        dscfg.setCheckpointFrequency(3_000);
        dscfg.setMaxWalArchiveSize(10 * GB);

        cfg.setDataStorageConfiguration(dscfg);

        return cfg;
    }

    /** */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();
    }

    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "1")
    public void checkInitPartitionWithConstantLoad() throws Exception {
//        int initCnt = 5_000 * PARTS_CNT;
//        int preloadCnt = initCnt * 2;
//        int totalCnt = preloadCnt * 2;

        IgniteEx node0 = startGrid(1);
        IgniteEx node1 = startGrid(2);

        node0.cluster().active(true);
        node0.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        IgniteInternalCache<Integer, Integer> cache = node0.cachex(DEFAULT_CACHE_NAME);

        AtomicLong cntr = new AtomicLong();

        ConstantLoader ldr = new ConstantLoader(node0.cache(DEFAULT_CACHE_NAME), cntr);

        IgniteInternalFuture ldrFut = GridTestUtils.runAsync(ldr);

        U.sleep(1_000);

        forceCheckpoint();

        // Switch to read-only node1
        GridCacheContext<Object, Object> cctx = node1.cachex(DEFAULT_CACHE_NAME).context();

        GridCachePreloadSharedManager preloader = node1.context().cache().context().preloader();

        GridCompoundFuture<Void,Void> destroyFut = new GridCompoundFuture<>();

        destroyFut.markInitialized();

        System.out.println(">>> switch to READ ONLY");

        // Destroy partitions.
        for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
            part.moving();

            // Simulating that part was downloaded and compltely destroying partition.
            destroyFut.add(preloader.schedulePartitionDestroy(part));
        }

        forceCheckpoint(node1);

        U.sleep(1_000);

        ldr.pause();

        forceCheckpoint(node0);

        List<GridDhtLocalPartition> parts = cache.context().topology().localPartitions();

        File[] partFiles = new File[parts.size()];

        for (GridDhtLocalPartition part : parts) {
            File src = new File(filePageStorePath(part));

            String node1filePath = filePageStorePath(node1.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(part.id()));

            File dest = new File(node1filePath +  ".tmp");

            System.out.println(">> copy " + src + " -> " + dest);

            RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

            GridFileUtils.copy(ioFactory, src, ioFactory, dest, Long.MAX_VALUE);

            partFiles[part.id()] = dest;
        }

        ldr.resume();

        U.sleep(1_000);

//        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

//        assertEquals(preloadCnt, cache.localSize(peekAll));

        // We can re-init partition just after destroy.
        destroyFut.get();

        long[] hwms = new long[cctx.topology().localPartitions().size()];
        long[] lwms = new long[hwms.length];
        int[] partsArr = new int[hwms.length];

        IgniteInternalFuture[] futs = new IgniteInternalFuture[hwms.length];

        System.out.println(">>> switch to FULL");

        // Restore partitions.
        for (GridDhtLocalPartition part : cctx.topology().localPartitions())
            futs[part.id()] = preloader.restorePartition(part.id(), partFiles[part.id()], cctx);

        forceCheckpoint(node1);

        for (int i = 0; i < hwms.length; i++) {
            T2<Long, Long> cntrPair = (T2<Long, Long>)futs[i].get();

            lwms[i] = cntrPair.get1();
            hwms[i] = cntrPair.get2();
            partsArr[i] = i;

            System.out.println(">>>> part " + i + " from " + lwms[i] + " to " + hwms[i]);
        }

        System.out.println("Triggering rebalancing ");

        preloader.triggerHistoricalRebalance(node0.localNode(), cctx, partsArr, lwms, hwms, PARTS_CNT);

        U.sleep(15_000);

        ldr.stop();

        ldrFut.get();

//        for (int i = preloadCnt; i < totalCnt; i++)
//            cache.put(i, i);
//
//        for (GridDhtLocalPartition part : cctx.topology().localPartitions())
//            assertEquals(totalCnt / cctx.topology().localPartitions().size(), part.fullSize());
//
//        validateLocal(node1.cachex(DEFAULT_CACHE_NAME), totalCnt);
    }

    /**
     * @param part Partition.
     * @return Absolute path to partition storage file.
     * @throws IgniteCheckedException If store doesn't exists.
     */
    private String filePageStorePath(GridDhtLocalPartition part) throws IgniteCheckedException {
        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)part.group().shared().pageStore();

        return ((FilePageStore)pageStoreMgr.getStore(part.group().groupId(), part.id())).getFileAbsolutePath();
    }

    private void validateLocal(IgniteInternalCache<Integer, Integer> cache, int size) throws IgniteCheckedException {
        assertEquals(size, cache.size());

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        for (int i = 0; i < size; i++)
            assertEquals(String.valueOf(i), Integer.valueOf(i), cache.localPeek(i, peekAll));
    }

    /** */
    private static class ConstantLoader implements Runnable {
        /** */
        private final AtomicLong cntr;

        /** */
        private volatile boolean pause;

        private volatile boolean paused;

        /** */
        private volatile boolean stop;

        /** */
        private final IgniteCache cache;

        /** */
        private final Set<Integer> rmvKeys = new HashSet<>();

        /** */
        private final Random rnd = ThreadLocalRandom.current();

        /** */
        public ConstantLoader(IgniteCache cache, AtomicLong cntr) {
            this.cache = cache;
            this.cntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stop) {
                if (pause) {
                    if (!paused)
                        paused = true;

                    try {
                        U.sleep(100);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        break;
                    }

                    continue;
                }

                long from = cntr.getAndAdd(100);

                for (long i = from; i < from + 100; i++) {
//                    boolean rmv0 = rmv.get();
//
//                    if (rmv0 != rmvPrev) {
//                        if (rmv0)
//                            rmvOffset = i;
//                        else
//                            rmvOffsetStop = i;
//
//                        rmvPrev = rmv0;
//                    }

                    cache.put(i, i);

//                    if (off > 0 && rmv0 && rnd.nextBoolean()) {
//                        int rmvKey = i - off;
//                        cache.remove(rmvKey);
//
//                        rmvKeys.add(rmvKey);
//                    }
                }

                try {
                    U.sleep(rnd.nextInt(10));
                }
                catch (IgniteInterruptedCheckedException e) {
                    break;
                }

//                off += cnt;
            }

//            int last = off - 1;
//
//            if (rmvOffsetStop == -1)
//                rmvOffsetStop = last;

//            return 0;
        }

//        public int rmvStopIdx() {
//            return rmvOffsetStop;
//        }
//
//        public int rmvStartIdx() {
//            return rmvOffset;
//        }

        public Set<Integer> rmvKeys() {
            return rmvKeys;
        }

        public void stop() {
            stop = true;
        }

        public void pause() {
            pause = true;

            while (!paused) {
                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void resume() {
            paused = false;
            pause = false;

        }
    }

    @Test
    @Ignore
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "1")
    public void checkInitPartition() throws Exception {
        int initCnt = 5_000 * PARTS_CNT;
        int preloadCnt = initCnt * 2;
        int totalCnt = preloadCnt * 2;

        IgniteEx node0 = startGrid(1);
        IgniteEx node1 = startGrid(2);

        node0.cluster().active(true);
        node0.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        IgniteInternalCache<Integer, Integer> cache = node0.cachex(DEFAULT_CACHE_NAME);

        for (int i = 0; i < initCnt; i++)
            cache.put(i, i);

        forceCheckpoint();

        GridCacheContext<Object, Object> cctx = node1.cachex(DEFAULT_CACHE_NAME).context();

        GridCachePreloadSharedManager preloader = node1.context().cache().context().preloader();

        GridCompoundFuture<Void,Void> destroyFut = new GridCompoundFuture<>();

        destroyFut.markInitialized();

        // Destroy partitions.
        for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
            part.moving();

            // Simulating that part was downloaded and compltely destroying partition.
            destroyFut.add(preloader.schedulePartitionDestroy(part));
        }

        forceCheckpoint(node1);

        for (int i = initCnt; i < preloadCnt; i++)
            cache.put(i, i);

        forceCheckpoint(node0);

        List<GridDhtLocalPartition> parts = cache.context().topology().localPartitions();

        File[] partFiles = new File[parts.size()];

        for (GridDhtLocalPartition part : parts) {
            File src = new File(filePageStorePath(part));

            String node1filePath = filePageStorePath(node1.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(part.id()));

            File dest = new File(node1filePath +  ".tmp");

            System.out.println(">> copy " + src + " -> " + dest);

            RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

            GridFileUtils.copy(ioFactory, src, ioFactory, dest, Long.MAX_VALUE);

            partFiles[part.id()] = dest;
        }

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        assertEquals(preloadCnt, cache.localSize(peekAll));

        // We can re-init partition just after destroy.
        destroyFut.get();

        // Restore partitions.
        for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
            IgniteInternalFuture<T2<Long, Long>> restoreFut =
                preloader.restorePartition(part.id(), partFiles[part.id()], cctx);

            forceCheckpoint(node1);

            assertTrue(restoreFut.isDone());

            assertEquals("Update counter validation", preloadCnt / PARTS_CNT, (long)restoreFut.get().get2());

            assertTrue(cctx.topology().own(part));

            assertEquals(OWNING, cctx.topology().partitionState(node1.localNode().id(), part.id()));
        }

        for (int i = preloadCnt; i < totalCnt; i++)
            cache.put(i, i);

        for (GridDhtLocalPartition part : cctx.topology().localPartitions())
            assertEquals(totalCnt / cctx.topology().localPartitions().size(), part.fullSize());

        validateLocal(node1.cachex(DEFAULT_CACHE_NAME), totalCnt);
    }
}
