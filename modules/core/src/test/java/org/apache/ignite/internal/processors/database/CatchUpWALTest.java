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

import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheDataStoreEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.util.IgniteUtils.GB;

/**
 *
 */
public class CatchUpWALTest extends GridCommonAbstractTest {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        //ccfg.setBackups(1);
        // todo check different sync modes
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

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

        cleanPersistenceDir();
    }

    @Test
    public void checkPartitionSwitchUnderConstantLoad() throws Exception {
        Ignite node = startGrids(2);

        node.cluster().active(true);

        node.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        AffinityTopologyVersion topVer = grid(0).context().cache().context().exchange().readyAffinityVersion();

//        fillCache(node, 0, 100);

        AtomicBoolean stopper = new AtomicBoolean();

        GridTestUtils.runAsync(new ConstantLoader(stopper, node));

        int primaryIdx =
            grid(0).cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(0).primary(topVer) ? 0 : 1;

        IgniteEx primaryNode = grid(primaryIdx);
        IgniteEx backupNode = grid(~primaryIdx & 1);

        log.info(">xxx> Primary: " + primaryNode.localNode().id());
        log.info(">xxx> Backup: " + backupNode.localNode().id());

        // set MOBING
        IgniteInternalCache backupCache = backupNode.cachex(DEFAULT_CACHE_NAME);
        GridDhtLocalPartition backupPart = backupCache.context().topology().localPartition(0);

        backupPart.moving();

        assert backupPart.state() == MOVING : backupPart.state();

        GridDhtPartitionMap backupPartsMap =
            backupNode.cachex(DEFAULT_CACHE_NAME).context().topology().localPartitionMap();

        primaryNode.cachex(DEFAULT_CACHE_NAME).context().topology().update(null, backupPartsMap, true);

//        GridCachePreloadSharedManager preloader = backupNode.context().cache().context().preloader();

//        IgniteInternalFuture fut = preloader.changePartitionsModeAsync(CacheDataStoreEx.StorageMode.READ_ONLY,
//            F.asMap(backupCache.context().group().groupId(), Collections.singleton(0)));

        U.sleep(200);

        backupNode.context().cache().context().database().checkpointReadLock();

        try {
            // Switching mode under the write lock.
            backupPart.dataStoreMode(CacheDataStoreEx.StorageMode.READ_ONLY);
        } finally {
            backupNode.context().cache().context().database().checkpointReadUnlock();
        }

        System.out.println(">xxx> switched to full");

        U.sleep(200);

        System.out.println("--- local entries: ");

        Iterable<Cache.Entry> it = backupCache.localEntries(new CachePeekMode[]{CachePeekMode.ALL});

        for (Cache.Entry e : it)
            System.out.println(">xx> " + e.getKey());

        backupNode.context().cache().context().database().checkpointReadLock();

        try {
            // Switching mode under the write lock.
            backupPart.dataStoreMode(CacheDataStoreEx.StorageMode.FULL);
        } finally {
            backupNode.context().cache().context().database().checkpointReadUnlock();
        }

        System.out.println("swicth to write");

        U.sleep(200);

        System.out.println("--- local entries2: ");

        it = backupCache.localEntries(new CachePeekMode[]{CachePeekMode.ALL});

        for (Cache.Entry e : it)
            System.out.println(">xx> " + e.getKey());

        stopper.set(true);

        U.sleep(1_000);

        System.out.println("Shutting down");
    }

    /** */
    @Test
    @Ignore
    public void checkCatchUpAndRecovery() throws Exception {
        Ignite node = startGrids(2);

        node.cluster().active(true);

        node.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        AffinityTopologyVersion topVer = grid(0).context().cache().context().exchange().readyAffinityVersion();

        fillCache(node, 0, 100);

        int primaryIdx =
            grid(0).cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(0).primary(topVer) ? 0 : 1;

        IgniteEx primaryNode = grid(primaryIdx);
        IgniteEx backupNode = grid(~primaryIdx & 1);

        log.info(">xxx> Primary: " + primaryNode.localNode().id());
        log.info(">xxx> Backup: " + backupNode.localNode().id());

        // set MOBING
        IgniteInternalCache backupCache = backupNode.cachex(DEFAULT_CACHE_NAME);
        GridDhtLocalPartition backupPart = backupCache.context().topology().localPartition(0);

        backupPart.moving();

        assert backupPart.state() == MOVING : backupPart.state();

        //backupNode.localNode()

        GridDhtPartitionMap bacupPartsMap = backupNode.cachex(DEFAULT_CACHE_NAME).context().topology().localPartitionMap();
        //System.out.println("xxx> " + map.get(0));

        primaryNode.cachex(DEFAULT_CACHE_NAME).context().topology().update(null, bacupPartsMap, true);
//
//        primaryNode.cachex(DEFAULT_CACHE_NAME).context().topology().partitionState()

        int size = backupCache.localSize(new CachePeekMode[] {CachePeekMode.ALL});

        log.info(">xxx> validating initial size");

        assertEquals(100, size);

        log.info(">xxx> initiating storage swithc to LOG_ONLY");

        IgniteCacheOffheapManager.CacheDataStore currStore = backupPart.dataStore(CacheDataStoreEx.StorageMode.FULL);

        // Pre-init the new storage.
        backupPart.dataStore(CacheDataStoreEx.StorageMode.READ_ONLY)
            .init(currStore.fullSize(), currStore.updateCounter(), currStore.cacheSizes());

        backupNode.context().cache().context().database().checkpointReadLock();

        try {
            // Switching mode under the write lock.
            backupPart.dataStoreMode(CacheDataStoreEx.StorageMode.READ_ONLY);
        } finally {
            backupNode.context().cache().context().database().checkpointReadUnlock();
        }

        assert backupPart.state() == MOVING : backupPart.state();

        log.info(">xxx> adding more entries to cache");

        fillCache(primaryNode, 100, 100);

//        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i+=10)
            primaryNode.cache(DEFAULT_CACHE_NAME).remove(i);

        assert backupPart.state() == MOVING : backupPart.state();

        U.sleep(1_000);

        log.info(">xxx> switching mode back");

        backupNode.context().cache().context().database().checkpointReadLock();

        try {
            // Switching mode under the write lock.
            backupPart.dataStoreMode(CacheDataStoreEx.StorageMode.FULL);
        } finally {
            backupNode.context().cache().context().database().checkpointReadUnlock();
        }

        log.info(">xxx> switching state back");

        backupPart.own();

        bacupPartsMap = backupNode.cachex(DEFAULT_CACHE_NAME).context().topology().localPartitionMap();
        //System.out.println("xxx> " + map.get(0));

        primaryNode.cachex(DEFAULT_CACHE_NAME).context().topology().update(null, bacupPartsMap, true);

        log.info(">xxx> getting size");

        size = backupCache.localSize(new CachePeekMode[] {CachePeekMode.ALL});

        System.out.println("backup size=" + size);

        IgniteInternalCache primaryCache = primaryNode.cachex(DEFAULT_CACHE_NAME);

        size = primaryCache.localSize(new CachePeekMode[] {CachePeekMode.ALL});

        System.out.println("primary size=" + size);

        //assert size == 200 : size;

    }


    /** */
    void fillCache(Ignite node, int off, int cnt) {
        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        for (int i = off; i < off + cnt; i++)
            cache.put(i, i);
    }

    private class ConstantLoader implements Runnable {

        private final AtomicBoolean stopper;
        private final Ignite node;

        public ConstantLoader(AtomicBoolean stopper, Ignite node) {
            this.stopper = stopper;
            this.node = node;
        }

        @Override public void run() {
            int n = 0;
            int cnt = 10;

            while (!stopper.get()) {
                fillCache(node, n, cnt);

                n += cnt;

                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();

                    break;
                }
            }

            System.out.println("finished on " + (n-1));
        }
    }
}
