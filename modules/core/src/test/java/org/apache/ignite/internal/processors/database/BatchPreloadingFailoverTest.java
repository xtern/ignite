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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_STORAGE_BATCH_PAGE_WRITE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class BatchPreloadingFailoverTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);
        dbCfg.setWalMode(WALMode.LOG_ONLY);

        dbCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
        );

        cfg.setDataStorageConfiguration(dbCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
//        ccfg.setRebalanceMode(SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        ccfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

//    @Override protected boolean isMultiJvm() {
//        return true;
//    }

    //
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** */
    private void loadPartition(GridCacheContext cctx, int p, Map<?, ?> data) throws IgniteCheckedException {
        GridDhtLocalPartition part = cctx.topology().localPartition(p);

        cctx.shared().database().checkpointReadLock();

        int cacheId = cctx.group().storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

        try {
            List<CacheDataRow> rows = new ArrayList<>(data.size());

            for (Map.Entry<?, ?> e : data.entrySet()) {
                KeyCacheObject key = cctx.toCacheKeyObject(e.getKey());
                CacheObject val = cctx.toCacheObject(e.getValue());

                key.valueBytes(cctx.cacheObjectContext());
                val.valueBytes(cctx.cacheObjectContext());

                if (key.partition() == -1)
                    key.partition(p);

                DataRow row = new DataRow(key, val, cctx.shared().versions().startVersion(), p, 0, cacheId);

                rows.add(row);
            }

            cctx.offheap().dataStore(part).rowStore().addRows(rows, cctx.group().statisticsHolderData());

            ((TcpDiscoverySpi)cctx.kernalContext().config().getDiscoverySpi()).simulateNodeFailure();

//            stopGrid(getTestIgniteInstanceName(0), true, false);
//
//            if (true) throw new RuntimeException("qqq");

            cctx.shared().database().checkpointReadUnlock();
        } finally {

        }
    }

    @Test
    @WithSystemProperty(key = IGNITE_DATA_STORAGE_BATCH_PAGE_WRITE, value = "true")
//    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "100")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void checkRecoveryMemoryLeakPart1() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        IgniteInternalCache cache = node0.cachex(DEFAULT_CACHE_NAME);

        Map<Integer, Integer> map = new LinkedHashMap<>();

        for (int i = 0; i < 1_000; i++)
            cache.put(i, i);

        for (int i = 1_000; i < 1_100; i++)
            map.put(i, i);

        try {
            loadPartition(cache.context(), 0, map);
        } catch (Exception e) {
            System.out.println("Catched runtime exception");
        }

        U.sleep(2_000);

        System.out.println("start node");

//        stopGrid(0);

        node0 = startGrid(0);

        node0.cluster().active(true);

        validateCache(node0.cachex(DEFAULT_CACHE_NAME).context());

    }

//    @Test
//    public void ass() throws Exception {
//
//    }

    private void validateCache(GridCacheContext cctx) throws IgniteCheckedException {
        try (GridCloseableIterator<Cache.Entry<Object, Object>> itr  = cctx.offheap().cacheEntriesIterator(cctx, true, true, cctx.topology().readyTopologyVersion(),
            false,
            null,
            true)) {

//            Thread.currentThread().stop();

            System.out.println("validate cache");

            while (itr.hasNext()) {
                Cache.Entry<Object, Object> entry = itr.next();

                Object key = entry.getKey();

//                System.out.println("key=" + key);

                assertTrue("Should not contain key=" + key, cctx.cache().containsKey(key));
            }

            System.out.println("success");
        }
    }
}
