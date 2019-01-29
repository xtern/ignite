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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class FreeListBatchUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int HDR_SIZE = 8 + 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setCacheMode(CacheMode.REPLICATED));

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testBatchPutAll() throws Exception {
        Ignite node = startGrid(0);

        int cnt = 200_000;
        int minSize = 0;
        int maxSize = 16384;
        int start = 0;

        log.info("Loading " + cnt + " random entries per " + minSize + " - " + maxSize + " bytes.");

        Map<Integer, byte[]> srcMap = new HashMap<>();

        for (int i = start; i < start + cnt; i++) {
            byte[] obj = generateObject(minSize + HDR_SIZE + ThreadLocalRandom.current().nextInt(maxSize - minSize) + 1);

            srcMap.put(i, obj);
        }

        try (IgniteDataStreamer<Integer, byte[]> streamer = node.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.addData(srcMap);
        }

        log.info("Done");

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        validateCacheEntries(cache, srcMap);

        final IgniteEx node2 = startGrid(1);

        log.info("await rebalance");

        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for ( GridDhtLocalPartition part : node2.context().cache().cache(DEFAULT_CACHE_NAME).context().group().topology().localPartitions()) {
                    if (part.state() != GridDhtPartitionState.OWNING)
                        return false;
                }

                return true;
            }
        }, 10_000);

        node.close();

        U.sleep(2_000);

        log.info("Verification on node2");

        validateCacheEntries(node2.cache(DEFAULT_CACHE_NAME), srcMap);
    }

    /**
     * @param cache Cache.
     * @param map Map.
     */
    @SuppressWarnings("unchecked")
    private void validateCacheEntries(IgniteCache cache, Map<Integer, byte[]> map) {
        assertEquals(map.size(), cache.size());

        for (Map.Entry<Integer, byte[]> e : map.entrySet()) {
            String idx = "idx=" + e.getKey();

            byte[] bytes = (byte[])cache.get(e.getKey());

            assertNotNull(idx, bytes);

            assertEquals(idx + ": length not equal", e.getValue().length, bytes.length);

            assertTrue(Arrays.equals(e.getValue(), bytes));
        }
    }

    /** */
    private byte[] generateObject(int size) {
        assert size >= HDR_SIZE : size;

        return new byte[size - HDR_SIZE];
    }
}
