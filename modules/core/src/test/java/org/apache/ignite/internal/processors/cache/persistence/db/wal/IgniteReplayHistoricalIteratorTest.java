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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteReplayHistoricalIteratorTest extends GridCommonAbstractTest {
    private static final int PARTS = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalSegmentSize(512 * 1024)
                .setWalMode(getWalMode())
                .setCheckpointFrequency(3_000)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        return cfg;
    }

    private WALMode getWalMode() {
        return WALMode.LOG_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc} Case is not relevant to the replay iterator.
     */
    @Test
    public void testHistoricalIterator() throws Exception {
        IgniteEx node0 = startGrids(2);

        node0.cluster().active(true);

        IgniteCache<Integer, Integer> cache = node0.cache(DEFAULT_CACHE_NAME);

        int initSize = 5_000;

        for (int i = 0; i < initSize; i++)
            cache.put(i, i);

        forceCheckpoint();

        AtomicInteger entriesCntr = new AtomicInteger(initSize);

        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int n = entriesCntr.getAndIncrement();

                try {
                    cache.put(n, n);
                }
                catch (Exception e) {
                    if (X.hasCause(e, IgniteInterruptedException.class)) {
                        System.out.println("n=" + n + " ignored");

                        Thread.currentThread().interrupt();
                    }
                    else
                        throw new RuntimeException(e);
                }
            }
        }, 2, "loader");

        GridCacheSharedContext<Object, Object> cctx = node0.context().cache().context();

        CacheGroupContext grp = cctx.cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        int entriesCnt = 100;

        reserveHistory(grp, entriesCnt);

        int retries = 2048;

        while (retries --> 0) {
            try {
                IgniteDhtDemandedPartitionsMap partCntrs = new IgniteDhtDemandedPartitionsMap();

                GridDhtLocalPartition part = grp.topology().localPartition(0);

                long to = part.updateCounter();
                long from = to - entriesCnt;

                partCntrs.addHistorical(0, from, to, 1);

                IgniteRebalanceIterator iter = grp.offheap().rebalanceIterator(partCntrs, grp.topology().readyTopologyVersion());

                try {
                    while (iter.hasNext())
                        iter.next();
                }
                finally {
                    iter.close();

                    System.out.println("left=" + retries + ", loaded=" + entriesCntr.get() + ", from=" + from + ", to=" + to);
                }

            }
            finally {
                node0.context().cache().context().database().releaseHistoryForPreloading();
            }
        }

        if (!fut.isDone())
            fut.cancel();

        fut.cancel();
    }


    private void reserveHistory(CacheGroupContext grp, int entriesCnt) {
        for (int p = 0; p < PARTS; p++) {
            GridDhtLocalPartition part0 = grp.topology().localPartition(p);

            long updCntr = part0.updateCounter();

            boolean reserved = grp.shared().cache().context().database().reserveHistoryForPreloading(grp.groupId(), p, updCntr - entriesCnt);

            assert reserved : "p=" + p + " minCntr=" + (updCntr - entriesCnt);
        }
    }
}
