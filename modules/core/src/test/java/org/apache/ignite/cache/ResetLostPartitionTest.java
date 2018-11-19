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

package org.apache.ignite.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class ResetLostPartitionTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** Cache name. */
    private static final String[] CACHE_NAMES = {"cacheOne", "cacheTwo", "cacheThree"};
    /** Cache size */
    public static final int CACHE_SIZE = 100000 / CACHE_NAMES.length;

    /** Persistence enabled flag. */
    private boolean persistenceEnabled = true;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(persistenceEnabled)
            .setMaxSize(300L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        CacheConfiguration[] ccfg = new CacheConfiguration[] {
            cacheConfiguration(CACHE_NAMES[0], CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_NAMES[1], CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_NAMES[2], CacheAtomicityMode.TRANSACTIONAL)
        };

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @param mode Cache atomicity mode.
     * @return Configured cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String cacheName, CacheAtomicityMode mode) {
        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(mode)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
            .setAffinity(new RendezvousAffinityFunction(false, 1024))
            .setIndexedTypes(String.class, String.class);
    }

    /** Client configuration */
    private IgniteConfiguration getClientConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(true);
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /**
     * Test to restore lost partitions after grid reactivation.
     *
     * @throws Exception if fail.
     */
    public void testReactivateGridBeforeResetLostPartitions() throws Exception {
        doRebalanceAfterPartitionsWereLost(true);
    }

    /**
     * Test to restore lost partitions on working grid.
     *
     * @throws Exception if fail.
     */
    public void testResetLostPartitions() throws Exception {
        doRebalanceAfterPartitionsWereLost(false);
    }

    /**
     * @param reactivateGridBeforeResetPart Reactive grid before try to reset lost partitions.
     * @throws Exception if fail.
     */
    private void doRebalanceAfterPartitionsWereLost(boolean reactivateGridBeforeResetPart) throws Exception {
        startGrids(3);

        grid(0).cluster().active(true);

        Ignite igniteClient = startGrid(getClientConfiguration("client"));

        for (String cacheName : CACHE_NAMES) {
            IgniteCache<Integer, String> cache = igniteClient.cache(cacheName);

            for (int j = 0; j < CACHE_SIZE; j++)
                cache.put(j, "Value" + j);
        }

        stopGrid("client");

        String dn2DirName = grid(1).name().replace(".", "_");

        stopGrid(1);

        //Clean up the pds and WAL for second data node.
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/" + dn2DirName, true));

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/wal/" + dn2DirName, true));

        //Here we have two from three data nodes and cache with 1 backup. So there is no data loss expected.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());

        //Start node 2 with empty PDS. Rebalance will be started.
        startGrid(1);

        //During rebalance stop node 3. Rebalance will be stopped.
        stopGrid(2);

        //Start node 3.
        startGrid(2);

        //Loss data expected because rebalance to node 1 have not finished and node 2 was stopped.
        assertTrue(CACHE_NAMES.length * CACHE_SIZE > averageSizeAroundAllNodes());

        for (String cacheName : CACHE_NAMES) {
            //Node 1 will have only OWNING partitions.
            assertTrue(getPartitionsStates(0, cacheName).stream().allMatch(state -> state == OWNING));

            //Node 3 will have only OWNING partitions.
            assertTrue(getPartitionsStates(2, cacheName).stream().allMatch(state -> state == OWNING));
        }

        boolean hasLost = false;
        for (String cacheName : CACHE_NAMES) {
            //Node 2 will have OWNING and LOST partitions.
            hasLost |= getPartitionsStates(1, cacheName).stream().anyMatch(state -> state == LOST);
        }

        assertTrue(hasLost);

        if (reactivateGridBeforeResetPart) {
            grid(0).cluster().active(false);
            grid(0).cluster().active(true);
        }

        //Try to reset lost partitions.
        grid(2).resetLostPartitions(Arrays.asList(CACHE_NAMES));

        awaitPartitionMapExchange();

        for (String cacheName : CACHE_NAMES) {
            //Node 2 will have only OWNING partitions.
            assertTrue(getPartitionsStates(1, cacheName).stream().allMatch(state -> state == OWNING));
        }

        //All data was back.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());

        //Stop node 2 for checking rebalance correctness from this node.
        stopGrid(2);

        //Rebalance should be successfully finished.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());
    }

    /**
     * Check that there is no duplicate partition owners after reset lost partitions.
     *
     * @throws Exception if fail.
     */
    public void testDuplicateOwners() throws Exception {
        persistenceEnabled = false;

        int gridCnt = 4;

        long timeout = 5_000;

        Ignite node = startGridsMultiThreaded(gridCnt);

        IgniteCache<Integer, Integer> cache = node.createCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE));

        for (int i = 0; i < CACHE_SIZE; i++)
            cache.put(i, i);

        int failedNodeIdx = gridCnt - 1;

        int lostPartsCnt = count(DEFAULT_CACHE_NAME, OWNING, failedNodeIdx);

        stopGrid(failedNodeIdx);

        int[] liveIdxs = new int[] {0, 1, 2};

        waitForCondition(() -> lostPartsCnt == count(DEFAULT_CACHE_NAME, LOST, liveIdxs), timeout);
        assertEquals(lostPartsCnt, count(DEFAULT_CACHE_NAME, LOST, liveIdxs));

        startGrid(failedNodeIdx);

        waitForCondition(() -> lostPartsCnt == count(DEFAULT_CACHE_NAME, LOST, failedNodeIdx), timeout);
        assertEquals(lostPartsCnt, count(DEFAULT_CACHE_NAME, LOST, failedNodeIdx));

        waitForCondition(() -> 0 == count(DEFAULT_CACHE_NAME, LOST, liveIdxs), timeout);
        assertEquals(0, count(DEFAULT_CACHE_NAME, LOST, liveIdxs));

        for (Ignite grid : G.allGrids()) {
            GridCacheSharedContext cctx = ((IgniteEx)grid).context().cache().context();

            cctx.exchange().affinityReadyFuture(cctx.discovery().topologyVersionEx()).get(timeout);

            for (GridCacheContext ctx : (Collection<GridCacheContext>)cctx.cacheContexts())
                ctx.preloader().rebalanceFuture().get(timeout);
        }

        node.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        waitForCondition(() -> lostPartsCnt == count(DEFAULT_CACHE_NAME, OWNING, failedNodeIdx), timeout);
        assertEquals(lostPartsCnt, count(DEFAULT_CACHE_NAME, OWNING, failedNodeIdx));

        int parts = grid(0).affinity(DEFAULT_CACHE_NAME).partitions();

        int[] allIdxs = new int[] {0, 1, 2, 3};

        waitForCondition(() -> parts == count(DEFAULT_CACHE_NAME, OWNING, allIdxs), timeout);
        assertEquals(parts, count(DEFAULT_CACHE_NAME, OWNING, allIdxs));
    }

    /**
     * @param gridNumber Grid number.
     * @param cacheName Cache name.
     * @return Partitions states for given cache name.
     */
    private List<GridDhtPartitionState> getPartitionsStates(int gridNumber, String cacheName) {
        CacheGroupContext cgCtx = grid(gridNumber).context().cache().cacheGroup(CU.cacheId(cacheName));

        GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)cgCtx.topology();

        return top.localPartitions().stream()
            .map(GridDhtLocalPartition::state)
            .collect(toList());
    }

    /**
     * Counts partitions in the specified state on the specified nodes.
     *
     * @param cacheName Cache name.
     * @param state Partition state.
     * @param gridIdx Grid index.
     * @return Number of local partitions in the specified state.
     */
    private int count(String cacheName, GridDhtPartitionState state, int ... gridIdx) {
        return Arrays.stream(gridIdx).map(idx ->
            getPartitionsStates(idx, cacheName).stream().filter(isEqual(state)).collect(toList()).size()).sum();
    }

    /**
     * Checks that all nodes see the correct size.
     */
    private int averageSizeAroundAllNodes() {
        int totalSize = 0;

        for (Ignite ignite : IgnitionEx.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                totalSize += ignite.cache(cacheName).size();
            }
        }

        return totalSize / IgnitionEx.allGrids().size();
    }
}
