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
import java.util.List;
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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.preload.GridCachePreloadSharedManager;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.io.GridFileUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
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

//    @Test
//    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "1")
//    public void checkInitPartitionWithConstantLoad() throws Exception

    @Test
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
            IgniteInternalFuture<Long> restoreFut =
                preloader.restorePartition(part.id(), partFiles[part.id()], cctx);

            forceCheckpoint(node1);

            assertTrue(restoreFut.isDone());

            assertEquals("Update counter validation", preloadCnt / PARTS_CNT, (long)restoreFut.get());

            assertTrue(cctx.topology().own(part));

            assertEquals(OWNING, cctx.topology().partitionState(node1.localNode().id(), part.id()));
        }

        for (int i = preloadCnt; i < totalCnt; i++)
            cache.put(i, i);

        for (GridDhtLocalPartition part : cctx.topology().localPartitions())
            assertEquals(totalCnt / cctx.topology().localPartitions().size(), part.fullSize());

        validateLocal(node1.cachex(DEFAULT_CACHE_NAME), totalCnt);
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
}
