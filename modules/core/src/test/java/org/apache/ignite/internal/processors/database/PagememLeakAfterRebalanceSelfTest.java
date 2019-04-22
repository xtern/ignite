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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;

/**
 * Check page memory
 */
public class PagememLeakAfterRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String DEF_CACHE_NAME = "defCacheName";

    private static final long DEF_REG_SIZE = 4 * 1024 * 1024 * 1024L;

    private CountDownLatch sync = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration def = new DataRegionConfiguration();
        def.setInitialSize(DEF_REG_SIZE);
        def.setMaxSize(DEF_REG_SIZE);
        def.setPersistenceEnabled(true);

        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        storeCfg.setDefaultDataRegionConfiguration(def);
//        storeCfg.setPageSize(pageSize);

//        if (persistence) {
//            storeCfg.setWalMode(WALMode.LOG_ONLY);
//            storeCfg.setMaxWalArchiveSize(Integer.MAX_VALUE);
//        }
//
        cfg.setDataStorageConfiguration(storeCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEF_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

//        if (getTestIgniteInstanceIndex(igniteInstanceName) == 0)
        cfg.setCommunicationSpi(new TestCommunicationSpi(getTestIgniteInstanceIndex(igniteInstanceName) == 0 ? sync : null));

        return cfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPreloadingWithConcurrentUpdates() throws Exception {
        int size = 5_000;

        Map<Integer, String> srcMap = new HashMap<>(U.capacity(size));

        Ignite node = startGrid(0);

        node.cluster().active(true);

        log.info("Load data");

        for (int i = 0; i < size; i++)
            srcMap.put(i, String.valueOf(i));

        node.cache(DEF_CACHE_NAME).putAll(srcMap);

        //node.cluster().active(false);

        IgniteEx node2 = startGrid(1);

        node.cluster().setBaselineTopology(node.cluster().nodes());

        //node.cluster().active(true);

        IgniteInternalCache<Object, Object> cache = null;

        while (cache == null)
            cache = node2.cachex(DEF_CACHE_NAME);

        for (int i = 0; i < size; i+=10) {
            String val = "key-" + i;

            cache.put(i, val);
            srcMap.put(i, val);

            // Start preloading.
            if (i > size / 2)
                sync.countDown();
        }

        awaitPartitionMapExchange();

        stopGrid(0);

        awaitPartitionMapExchange();

        log.info("Verfify cache contents.");

        assertEquals(srcMap.size(), cache.size());

        checkDuplicates(cache.context(), srcMap);
    }

    private void checkDuplicates(GridCacheContext cctx, Map<Integer, String> srcMap) throws IgniteCheckedException {
        try (GridCloseableIterator<Cache.Entry<Object, Object>> itr = cctx.offheap().cacheEntriesIterator(cctx, true, true, cctx.topology().readyTopologyVersion(),
            false,
            null,
            true)) {

            while (itr.hasNext()) {
                Cache.Entry<Object, Object> entry = itr.next();

                Object key = entry.getKey();

                assertEquals(srcMap.remove(key), entry.getValue());
            }

            assertTrue(srcMap.isEmpty());
        }
    }

    /** */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final CountDownLatch latch;

        /** */
        private TestCommunicationSpi(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Message msg0 = ((GridIoMessage)msg).message();

                if (latch != null && latch.getCount() > 0 && msg0 instanceof GridDhtPartitionSupplyMessage) {
                    try {
                        latch.await(10, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteSpiException(e);
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }

    }
}
