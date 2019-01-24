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
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.U;
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

        int max = 200_000;

        //try () {
        Map<Integer, Object> data = randomData(0, max, 0,2048);

        log.info("Loading 200k");

        node.cache(DEFAULT_CACHE_NAME).putAll(data);


//
//        try (IgniteDataStreamer<Integer, Object> streamer = node.dataStreamer(DEFAULT_CACHE_NAME)) {
//            streamer.addData(data);
//        }

        log.info("Done");

//        data = new IdentityHashMap<>();
//
//        int[] sizes = {42, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 2048};

//            int sum = 0, pageSize = 4096, start = 64, idx = 0;
//
//            while ((sum + start) <= pageSize) {
////                if (sum > start)
//
//
//                sum += start;
//
//                sizes[idx++] = start;
//
//                start *= 2;
//            }
//
//            assert sum + 64 == pageSize : sum;
//
//            int off = 10_000;
//
//            int end = off + ((65_536 / sizes.length) * sizes.length);
//
//            for (int i = off; i < end; i++) {
//                int objSize = sizes[sizes.length - 1 - ((i - off) % sizes.length)];
//                if (objSize == 64)
//                    objSize = 42;
//
//                data.put(i, generateObject(objSize));
//            }
//
//            long startTime = U.currentTimeMillis();
//
//            node.cache(DEFAULT_CACHE_NAME).putAll(data);
//
//            log.info("Done: " + (U.currentTimeMillis() - startTime) + " ms.");

//            GridDhtLocalPartition.DBG = true;

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        assert cache.size() == max : cache.size();

        for (int i = 0; i < max; i++)
            assert cache.get(i) != null : i;

        IgniteEx node2 = startGrid(1);

        log.info("await rebalance");

        for (IgniteInternalCache cache0 : node2.context().cache().caches())
            cache0.context().preloader().rebalanceFuture().get();



        log.info("starting verification on node2");

        cache = node2.cache(DEFAULT_CACHE_NAME);

        assert cache.size() == max : cache.size();

        for (int i = 0; i < max; i++)
            assert cache.get(i) != null : i;

//        U.sleep(10_000);

        log.info("stop crd");

        stopGrid(0);

        log.info("There is someone following you");

        log.info("Stopping last standing");

        stopGrid(0);
//        }
    }

    @Test
    public void checkFreeList() throws Exception {
        try (IgniteEx node = startGrid(0)) {
            IgniteInternalCache cache = node.cachex(DEFAULT_CACHE_NAME);

            GridCacheContext cctx = cache.context();

//            cctx.offheap().updateBatch(cctx, );
        }
    }

    /** */
    private Map<Integer, Object> randomData(int start, int size, int minSize, int maxSize) {
        Map<Integer, Object> res = new HashMap<>();

        for (int i = start; i < start + size; i++) {
            Object obj = generateObject(minSize + HDR_SIZE + ThreadLocalRandom.current().nextInt(maxSize - minSize) + 1);

            res.put(i, obj);
        }

        return res;
    }

    /** */
    private Object generateObject(int size) {
        assert size >= HDR_SIZE : size;

        return new byte[size - HDR_SIZE];
    }
}
