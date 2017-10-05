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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
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
 * Test that compute task will be rejected when executed within transaction/lock if topology pending updates.
 */
public class IgniteComputeWithPendingTopologyDeadlockTest extends GridCommonAbstractTest {

    /** Maximum timeout for topology update (prevent hanging and long shutdown). */
    private static final long PENDING_TIMEOUT = 15_000L;

    /** */
    private static final long CACHE_CREATION_TIMEOUT = 2_000L;

    /** */
    private static final int CACHE_SIZE = 10;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /** */
    private static final CacheMode[] TEST_CACHE_MODES = { CacheMode.PARTITIONED, CacheMode.REPLICATED };

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /**
     * Create cache configuration with transactional atomicity mode.
     *
     * @param name Cache name.
     * @param mode {@link CacheMode Cache mode.}
     * @param writeMode {@link CacheWriteSynchronizationMode Cache write synchronization mode.}
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheMode mode,
        CacheWriteSynchronizationMode writeMode) {
        CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>(name);

        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheConfiguration.setCacheMode(mode);
        cacheConfiguration.setWriteSynchronizationMode(writeMode);

        return cacheConfiguration;
    }

    /**
     * Explicit lock test.
     *
     * @throws Exception If fails.
     */
    public void testExplicitLock() throws Exception {
        for (CacheMode mode : TEST_CACHE_MODES) {
            for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
                boolean result = doTestExplicitLock(mode, syncMode);

                String msg = "Possible deadlock in this mode: " + mode + "/" + syncMode + " (see thread dumps).";

                assertTrue(msg, result);
            }
        }
    }


    /**
     * Transactional lock test.
     *
     * @throws Exception If fails.
     */
    public void testTxLock() throws Exception {
        for (CacheMode cacheMode : TEST_CACHE_MODES) {
            for (CacheWriteSynchronizationMode cacheWriteSyncMode : CacheWriteSynchronizationMode.values()) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        boolean result = doTestTx(concurrency, isolation, cacheMode, cacheWriteSyncMode);

                        String msg = "Possible deadlock in this modes:" +
                            " cache - " + cacheMode + "/" + cacheWriteSyncMode + ", "
                            + "tx - " + concurrency + "/" + isolation + " (see thread dumps).";

                        assertTrue(msg, result);
                    }
                }
            }
        }
    }

    /**
     * Test execution within lock.
     *
     * @param mode {@link CacheMode Cache mode.}
     * @param writeMode {@link CacheWriteSynchronizationMode Cache write synchronization mode.}
     * @return {@code True} if no hanging was detected.
     * @throws Exception if fails.
     */
    private boolean doTestExplicitLock(CacheMode mode, CacheWriteSynchronizationMode writeMode) throws Exception {
        final Ignite node = grid(0);

        final String name1 = CACHE1 + mode + writeMode;
        final String name2 = CACHE2 + mode + writeMode;
        final String name3 = CACHE3 + mode + writeMode;

        final IgniteCache<Integer, Integer> cache1 = node.createCache(cacheConfiguration(name1, mode, writeMode));
        final IgniteCache<Integer, Integer> cache2 = node.createCache(cacheConfiguration(name2, mode, writeMode));

        final CountDownLatch sync = new CountDownLatch(1);

        putTestEntries(cache1);
        putTestEntries(cache2);

        IgniteInternalFuture<Boolean> fut = runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws IgniteInterruptedCheckedException {
                Lock lock = cache1.lock(2);

                lock.lock();

                try {
                    sync.countDown();

                    // Await when new cache will be created.
                    boolean done = GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return node.cacheNames().contains(name3);
                        }
                    }, CACHE_CREATION_TIMEOUT);

                    assertTrue(done);

                    // Execute distributed (topology version aware) task.
                    cache2.size();
                }
                catch (CacheException ignore) {
                    return true;
                }
                finally {
                    lock.unlock();
                }
                return false;
            }
        });

        // Create watch-thread that will cancel task if timeout expired.
        IgniteInternalFuture<Boolean> trackingFuture = startTracking(fut);

        // Await new thread.
        sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

        // Create cache - topology update.
        node.createCache(name3);

        // Await completion or timeout.
        return trackingFuture.get(PENDING_TIMEOUT * 2);
    }

    /**
     * Test execution within transaction.
     *
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     * @return {@code True} if no hanging was detected.
     * @throws Exception If fails.
     */
    private boolean doTestTx(
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        final CacheMode cacheMode,
        final CacheWriteSynchronizationMode cacheWriteMode) throws Exception {
        final Ignite node = grid(0);

        final String name1 = CACHE1 + cacheMode + cacheWriteMode + concurrency + isolation;
        final String name2 = CACHE2 + cacheMode + cacheWriteMode + concurrency + isolation;
        final String name3 = CACHE3 + cacheMode + cacheWriteMode + concurrency + isolation;

        final IgniteCache<Integer, Integer> cache1 = node.createCache(cacheConfiguration(name1, cacheMode, cacheWriteMode));
        final IgniteCache<Integer, Integer> cache2 = node.createCache(cacheConfiguration(name2, cacheMode, cacheWriteMode));

        final CountDownLatch sync = new CountDownLatch(1);

        putTestEntries(cache1);
        putTestEntries(cache2);

        IgniteInternalFuture<Boolean> fut = runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws IgniteInterruptedCheckedException {
                try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                    cache1.put(12, 12);

                    sync.countDown();

                    // Await when new cache will be created.
                    boolean done = GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return node.cacheNames().contains(name3);
                        }
                    }, CACHE_CREATION_TIMEOUT);

                    assertTrue(done);

                    // Execute distributed (topology version aware) task.
                    cache2.clear();

                    tx.commit();
                }
                catch (CacheException ignore) {
                    return concurrency == TransactionConcurrency.PESSIMISTIC;
                }

                return concurrency == TransactionConcurrency.OPTIMISTIC;
            }
        });
        // Create watch-thread that will cancel task if timeout expired.
        IgniteInternalFuture<Boolean> trackingFuture = startTracking(fut);

        // Await new thread.
        sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

        // Create cache - topology update.
        node.createCache(cacheConfiguration(name3, cacheMode, cacheWriteMode));

        // Await completion or timeout.
        return trackingFuture.get(PENDING_TIMEOUT * 2);
    }

    /**
     * Put rest keys to cache.
     *
     * @param cache Cache to fill with test entries.
     */
    private void putTestEntries(Cache<Integer, Integer> cache) {
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
            @Override public Boolean call() throws IgniteCheckedException {
                try {
                    return fut.get(PENDING_TIMEOUT);
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    U.dumpThreads(log);

                    fut.cancel();

                    return false;
                }
            }
        }, "Deadlock tracker");
    }

}
