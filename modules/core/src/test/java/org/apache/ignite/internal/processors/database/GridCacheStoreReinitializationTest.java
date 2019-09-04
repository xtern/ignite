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
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheDataStoreEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.io.GridFileUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.IgniteUtils.GB;

/**
 *
 */
public class GridCacheStoreReinitializationTest extends GridCommonAbstractTest {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));
        //ccfg.setBackups(1);
        // todo check different sync modes
//        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dscfg = new DataStorageConfiguration();

        DataRegionConfiguration reg = new DataRegionConfiguration();

        reg.setMaxSize(2 * GB);
        reg.setPersistenceEnabled(true);

        dscfg.setDefaultDataRegionConfiguration(reg);
        dscfg.setCheckpointFrequency(3_000);

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
    public void checkInitPartition() throws Exception {
        Ignite node = startGrids(2);

        node.cluster().active(true);

        node.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        AffinityTopologyVersion topVer = grid(0).context().cache().context().exchange().readyAffinityVersion();

        AtomicBoolean stopper = new AtomicBoolean();

        // todo
        //fillCache(node, 0, 10);

        int primaryIdx =
            grid(0).cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(0).primary(topVer) ? 0 : 1;

        int backupIdx = ~primaryIdx & 1;

        IgniteEx primaryNode = grid(primaryIdx);
        IgniteEx backupNode = grid(backupIdx);

        log.info(">xxx> Primary: " + primaryNode.localNode().id());
        log.info(">xxx> Backup: " + backupNode.localNode().id());

        // set MOBING
        IgniteInternalCache<Integer, Integer> backupCache = backupNode.cachex(DEFAULT_CACHE_NAME);
        GridDhtLocalPartition backupPart = backupCache.context().topology().localPartition(0);

        backupPart.moving();

        assert backupPart.state() == MOVING : backupPart.state();

        GridDhtPartitionMap backupPartsMap = backupCache.context().topology().localPartitionMap();

        backupCache.context().topology().update(null, backupPartsMap, true);

        backupNode.context().cache().context().database().checkpointReadLock();

        try {
            // Switching mode under the write lock.
            backupPart.dataStoreMode(CacheDataStoreEx.StorageMode.READ_ONLY);
        } finally {
            backupNode.context().cache().context().database().checkpointReadUnlock();
        }

        final int grpId = backupCache.context().group().groupId();
        final int backupPartId = backupPart.id();

        // Simulating that part was downloaded and compltely destroying partition.
        destroyPartition(backupNode, backupPart);

        String partPath = System.getProperty("PART_FILE_LOCATION");

        assert partPath != null : "PART_FILE_LOCATION not defined";

        overwritePartitionFile(backupIdx, backupPartId, partPath);

        // Free off-heap.
        ((PageMemoryEx)backupCache.context().group().dataRegion().pageMemory())
            .clearAsync((grp, pageId) -> grp == grpId && backupPartId == PageIdUtils.partId(pageId),true)
            .get();

        AffinityTopologyVersion affVer = backupCache.context().topology().readyTopologyVersion();

        // Create partition.
        backupPart = backupCache.context().topology().localPartition(0, affVer, true, true);

        log.info(">xxx> Re-initialize partition: " + backupPart.state());

//        backupCache.context().shared().pageStore().ensure(backupCache.context().group().groupId(), 0);

        backupPart.dataStore().init(0, 0, null);

        System.out.println(">xxx> Own partition: " + backupPartId);

        backupCache.context().topology().own(backupPart);

        assert backupPart.state() == OWNING : backupPart.state();

        GridDhtPartitionState partState = backupCache.context().topology().partitionState(backupNode.localNode().id(), backupPartId);

        System.out.println(">xxx> node2part state " + partState);

//        System.out.println(">xxx> force checkpoint");
//        forceCheckpoint();

        validateLocal(backupCache, 10_000);

//        File destFile = partitionFile(backupNode, DEFAULT_CACHE_NAME, 0);

//        GridCachePreloadSharedManager preloader = backupNode.context().cache().context().preloader();
//
//        preloader.reInitialize(p, new File("~/check-rebalance/file-rebalance/part-0.bin.tmp"));

        System.out.println("shutting down");

        U.sleep(1_000);
    }

    private void validateLocal(IgniteInternalCache<Integer, Integer> cache, int size) throws IgniteCheckedException {
        assertEquals(size, cache.size());

        for (int i = 0; i < size; i++)
            assertEquals(String.valueOf(i), Integer.valueOf(i), cache.get(i));
    }

    private void overwritePartitionFile(int nodeIdx, int partId, String srcPath) throws IgniteCheckedException, IOException {
        File dst = partitionFile(grid(nodeIdx), DEFAULT_CACHE_NAME, partId, nodeIdx);

        File src = new File(srcPath);

        log.info(">> copy files: \n\t\t" + src + "\n\t\t   \\---> " + dst);

        RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

        GridFileUtils.copy(ioFactory, src, ioFactory, dst, Long.MAX_VALUE);
    }

    private void destroyPartition(Ignite node, GridDhtLocalPartition part) throws InterruptedException, IgniteCheckedException {
        CountDownLatch waitRent = new CountDownLatch(1);

        part.rent(false);

        part.onClearFinished(f -> waitRent.countDown());

        waitRent.await();

        forceCheckpoint(node);
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param partId Partition id.
     */
    private static File partitionFile(Ignite ignite, String cacheName, int partId, int backupIndex) throws IgniteCheckedException {
        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeName = "node0" + backupIndex + "-" + ignite.cluster().localNode().consistentId();

        return new File(dbDir, String.format("%s/cache-%s/part-%d.bin", nodeName, cacheName, partId));
    }
}
