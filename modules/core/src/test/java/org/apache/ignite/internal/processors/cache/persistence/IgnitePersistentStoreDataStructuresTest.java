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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePersistentStoreDataStructuresTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setAutoActivationEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueue() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteQueue<Object> queue = ignite.queue("testQueue", 100, new CollectionConfiguration());

        for (int i = 0; i < 100; i++)
            queue.offer(i);

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        queue = ignite.queue("testQueue", 0, null);

        for (int i = 0; i < 100; i++)
            assertEquals(i, queue.poll());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteAtomicLong atomicLong = ignite.atomicLong("testLong", 0, true);

        for (int i = 0; i < 100; i++)
            atomicLong.incrementAndGet();

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        atomicLong = ignite.atomicLong("testLong", 0, false);

        for (int i = 100; i != 0; )
            assertEquals(i--, atomicLong.getAndDecrement());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSequence() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteAtomicSequence sequence = ignite.atomicSequence("testSequence", 0, true);

        int i = 0;

        while (i < 1000) {
            sequence.incrementAndGet();

            i++;
        }

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        sequence = ignite.atomicSequence("testSequence", 0, false);

        assertTrue(sequence.incrementAndGet() > i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSet() throws Exception {
        Ignite ignite = startGrids(4);

        int total = 200_000;
        int updCnt = 10_000;

        int max = total + updCnt;

        List<Integer> data = new ArrayList<>(max);

        for (int i = 0; i < max; i++)
            data.add(i);

        List<Integer> initData = data.subList(0, total);

        ignite.cluster().active(true);

        IgniteSet<Integer> set = ignite.set("testSet", new CollectionConfiguration());

        set.addAll(initData);

        assertEquals(total, set.size());

        stopAllGrids();

        // Prepare new data for update after initialization.
        List<Integer> rmvData = data.subList(0, updCnt);

        List<Integer> addData = data.subList(total, max);

        ignite = startGrids(4);

        ignite.cluster().active(true);

        final IgniteSet<Integer> set0 = ignite.set("testSet", null);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            set0.removeAll(rmvData);
        });

        set0.addAll(addData);

        fut.get();

        assertEquals(total, set0.size());

        assertFalse(set0.add(max - 1));

        // Check iterator.
        Set<Integer> exp = new HashSet<>(data.subList(updCnt, max));
        Set<Integer> actual = new HashSet<>();

        for (Integer num : set0)
            actual.add(num);

        assertEquals(exp, actual);

        ignite.cluster().active(false);

        ignite.cluster().active(true);

        set = ignite.set("testSet", null);

        // Check operations reordering.
        for (int i = 0; i < updCnt; i++) {
            set.add(i);

            set.remove(i);
        }

        assertEquals(total, set.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockVolatility() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteLock lock = ignite.reentrantLock("test", false, true, true);

        assert lock != null;

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        lock = ignite.reentrantLock("test", false, true, false);

        assert lock == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSemaphoreVolatility() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteSemaphore sem = ignite.semaphore("test", 10, false, true);

        assert sem != null;

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        sem = ignite.semaphore("test", 10, false, false);

        assert sem == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchVolatility() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteCountDownLatch latch = ignite.countDownLatch("test", 10, false, true);

        assert latch != null;

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        latch = ignite.countDownLatch("test", 10, false, false);

        assert latch == null;
    }

}
