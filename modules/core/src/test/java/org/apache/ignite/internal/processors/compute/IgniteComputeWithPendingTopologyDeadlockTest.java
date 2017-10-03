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

package org.apache.ignite.internal.processors.compute;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Reproduce <a href="https://issues.apache.org/jira/browse/IGNITE-6380">IGNITE-6380</a>
 * (restrict execution within transaction or lock if topology pending updates).<br>
 *
 * The main goal for tests is to fail, when deadlock possible.
 */
public class IgniteComputeWithPendingTopologyDeadlockTest extends GridCommonAbstractTest {

    /** Maximum timeout for topology update (prevent hanging and long shutdown). */
    private static final long PENDING_TIMEOUT = 15_000;

    /** */
    private static final long CACHE_CREATION_TIMEOUT = 2_000;

    /** */
    private static final int CACHE_SIZE = 10;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /**
     * Create cache configuration with transactional atomicity mode.
     *
     * @param mode name for cache.
     * @return cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheMode mode,
        CacheWriteSynchronizationMode writeMode) {

        return new CacheConfiguration<Integer, Integer>(name)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(mode)
            .setWriteSynchronizationMode(writeMode);
    }

    /**
     * Explicit lock test.
     *
     * @throws Exception if fails.
     */
    public void testExplicitLock() throws Exception {
        for (CacheMode cacheMode : CacheMode.values())
            for (CacheWriteSynchronizationMode cacheWriteSyncMode : CacheWriteSynchronizationMode.values())
                assertTrue("Possible deadlock in this mode: " + cacheMode + "/"
                    + cacheWriteSyncMode + " (see thread dumps).", doTestExplicitLock(cacheMode, cacheWriteSyncMode));
    }


    /**
     * Transactional lock test.
     *
     * @throws Exception if fails.
     */
    public void testTxLock() throws Exception {
        for (CacheMode cacheMode : CacheMode.values())
            for (CacheWriteSynchronizationMode cacheWriteSyncMode : CacheWriteSynchronizationMode.values())
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values())
                        assertTrue("Possible deadlock in this modes:" +
                                " cache - " + cacheMode + "/" + cacheWriteSyncMode + ", "
                                + "tx - " + concurrency + "/" + isolation + " (see thread dumps).",
                            doTestTx(concurrency, isolation, cacheMode, cacheWriteSyncMode));
    }

    /**
     * Test ensures that tasks executes normally within transaction without pending topology.
     *
     * @throws Exception if fails.
     */
    public void testWithoutPendingTopology() throws Exception {
        for (CacheMode cacheMode : CacheMode.values())
            for (CacheWriteSynchronizationMode cacheWriteSyncMode : CacheWriteSynchronizationMode.values())
                doTestWithoutPendingTopology(cacheMode, cacheWriteSyncMode);
    }

    /**
     * Test execution within transaction.
     *
     * @param concurrency transaction concurrency level.
     * @param isolation transaction isolation level.
     * @return {@code True} if no hanging was detected.
     * @throws Exception if fails.
     */
    private boolean doTestTx(
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        final CacheMode cacheMode,
        final CacheWriteSynchronizationMode cacheWriteMode) throws Exception {

        try (Ignite node = startGrid(0)) {
            final IgniteCache<Integer, Integer> cache1 = node.createCache(cacheConfiguration(CACHE1, cacheMode, cacheWriteMode));
            final IgniteCache<Integer, Integer> cache2 = node.createCache(cacheConfiguration(CACHE2, cacheMode, cacheWriteMode));

            // Fill with test values.
            fillCaches(ImmutableList.of(cache1, cache2));

            final CountDownLatch sync = new CountDownLatch(1);

            IgniteInternalFuture<Boolean> fut = runAsync(new Callable<Boolean>() {
                @Override public Boolean call() throws IgniteInterruptedCheckedException {
                    // Transaction manager starts on node1.
                    try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {

                        cache1.put(12, 12);

                        // Signal main thread.
                        sync.countDown();

                        // Await when new cache will be created.
                        boolean done = GridTestUtils.waitForCondition(new PA() {
                            @Override public boolean apply() {
                                return node.cacheNames().size() >= 3;
                            }
                        }, CACHE_CREATION_TIMEOUT);

                        assertTrue(done);

                        // Execute distributed (topology version aware) task.
                        cache2.clear();

                        tx.commit();
                    }
                    catch (CacheException ignore) {
                        // Expected behavior - computation was aborted.
                    }
                    return true;
                }
            });
            // Create watch-thread that will cancel task if timeout expired.
            IgniteInternalFuture<Boolean> trackingFuture = startTracking(fut);

            // Await new thread.
            sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

            // Create cache - topology update.
            node.createCache(cacheConfiguration(CACHE3, cacheMode, cacheWriteMode));

            // Await completion or timeout.
            return trackingFuture.get();
        }
    }

    /**
     * Test execution within lock.
     *
     * @return {@code True} if no hanging was detected.
     * @throws Exception if fails.
     */
    private boolean doTestExplicitLock(CacheMode cacheMode,
        CacheWriteSynchronizationMode cacheWriteMode) throws Exception {
        try (Ignite node = startGrid(0)) {

            final IgniteCache<Integer, Integer> cache1 = node.createCache(cacheConfiguration(CACHE1, cacheMode, cacheWriteMode));
            final IgniteCache<Integer, Integer> cache2 = node.createCache(cacheConfiguration(CACHE2, cacheMode, cacheWriteMode));

            // Fill with test values.
            fillCaches(ImmutableList.of(cache1, cache2));

            final CountDownLatch sync = new CountDownLatch(1);

            IgniteInternalFuture<Boolean> fut = runAsync(new Callable<Boolean>() {

                @Override public Boolean call() throws IgniteInterruptedCheckedException {

                    Lock lock = cache1.lock(2);

                    lock.lock();

                    try {
                        // Signal main thread.
                        sync.countDown();

                        // Await when new cache will be created.
                        boolean done = GridTestUtils.waitForCondition(new PA() {
                            @Override public boolean apply() {
                                return node.cacheNames().size() >= 3;
                            }
                        }, CACHE_CREATION_TIMEOUT);

                        assertTrue(done);

                        // Execute distributed (topology version aware) task.
                        cache2.size();
                    }
                    catch (CacheException ignore) {
                        // Expected behavior - computation was aborted.
                    }
                    finally {
                        lock.unlock();
                    }
                    return true;
                }
            });

            // Create watch-thread that will cancel task if timeout expired.
            IgniteInternalFuture<Boolean> trackingFuture = startTracking(fut);

            // Await new thread.
            sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

            // Create cache - topology update.
            node.createCache(CACHE3);

            // Await completion or timeout.
            return trackingFuture.get();
        }
    }

    /**
     * Ensure that tasks executes normally without pending topology.
     *
     * @param cacheMode cache mode.
     * @param cacheWriteMode cache write synchronization mode.
     * @throws Exception if fails.
     */
    private void doTestWithoutPendingTopology(CacheMode cacheMode,
        CacheWriteSynchronizationMode cacheWriteMode) throws Exception {
        int sizeBefore, sizeAfter;

        try (Ignite node = startGrid(0)) {

            final IgniteCache<Integer, Integer> cache = node.createCache(cacheConfiguration(CACHE1, cacheMode, cacheWriteMode));

            // Fill with test values.
            fillCaches(Collections.singletonList(cache));

            try (Transaction tx = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE)) {

                cache.put(12, 12);

                sizeBefore = cache.size();

                cache.clearAll(ImmutableSet.of(1, 2, 3, 4, 12));

                tx.commit();
            }

            sizeAfter = cache.size();
        }

        // First put is not visible for distributed task till end of transaction.
        assertEquals(cacheMode + "/" + cacheWriteMode, CACHE_SIZE, sizeBefore);
        assertEquals(cacheMode + "/" + cacheWriteMode, 7, sizeAfter);
    }

    /**
     * Populate cache with test keys.
     *
     * @param caches caches to fill with test entries.
     */
    private void fillCaches(List<IgniteCache<Integer, Integer>> caches) {
        for (IgniteCache<Integer, Integer> cache : caches)
            for (int i = 0; i < CACHE_SIZE; i++)
                cache.put(i, i);
    }

    /**
     * Start timeout tracking.
     *
     * @param fut {@code IgniteInternalFuture} to track.
     * @return {@code IgniteInternalFuture<Boolean>} that will return timeout status.
     */
    private IgniteInternalFuture<Boolean> startTracking(final IgniteInternalFuture<Boolean> fut) {
        return runAsync(new Callable<Boolean>() {
            @Override public Boolean call() {
                try {
                    // Busy wait for completion.
                    boolean done = GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return fut.isDone();
                        }
                    }, PENDING_TIMEOUT);

                    // If task was finished with uncaught exception.
                    if (fut.error() != null) {
                        log.error(fut.error().getMessage(), fut.error());
                        return false;
                    }

                    // If timeout expired.
                    if (!done) {
                        U.dumpThreads(log);
                        fut.cancel();
                        return false;
                    }

                    return true;
                }
                catch (IgniteCheckedException e) {
                    log.error(e.getMessage(), e);
                    fail("Tracking thread abnormal termination");
                    return false;
                }
            }
        }, "Deadlock tracker");
    }

}