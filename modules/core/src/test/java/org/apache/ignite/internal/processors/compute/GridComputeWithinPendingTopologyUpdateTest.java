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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Reproduce different cases for <a href="https://issues.apache.org/jira/browse/IGNITE-6380">IGNITE-6380</a>
 * (restrict execution within transaction or lock if topology pending updates).
 */
public class GridComputeWithinPendingTopologyUpdateTest extends GridCommonAbstractTest {

    /** Maximum timeout for topology update (prevent hanging and long shutdown). */
    private static final long PENDING_TIMEOUT = 10_000;

    /** */
    private static final long CACHE_CREATION_TIMEOUT = 2_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setUserAttributes(F.asMap("EVEN", gridName.endsWith("0")));
    }

    /**
     * Create cache with transactional atomicity mode.
     *
     * @param cacheName name for cache.
     * @param <K> type of key.
     * @param <V> type of value.
     * @return cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> txCacheConf(String cacheName) {
        return new CacheConfiguration<K, V>(cacheName)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new TestAffinityFunction());
    }

    /**
     * PESSIMISTIC / READ_COMMITTED read only - no locks, expected normal execution of computable task
     *
     * @throws Exception if fails.
     */
    public void testComputePessimisticReadCommitedReadOnly() throws Exception {
        boolean restricted = txBase(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);

        // clear must complete without exception
        assert !restricted;
    }

    /**
     * PESSIMISTIC / READ_COMMITTED with write lock, expected exception on task execution.
     *
     * @throws Exception if fails.
     */
    public void testComputePessimisticReadCommited() throws Exception {
        boolean restricted = txBase(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, true, true);

        // Task must abort with exception.
        assert restricted;
    }

    /**
     * PESSIMISTIC / REPEATABLE_READ with read lock, expected exception on task execution.
     *
     * @throws Exception if fails.
     */
    public void testComputePessimisticRepeatableRead() throws Exception {
        boolean restricted = txBase(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        // Task must abort with exception.
        assert restricted;
    }

    /**
     * OPTIMISTIC / SERIALIZABLE - no exception expected on task execution.
     *
     * @throws Exception if fails.
     */
    public void testComputeOptimisticSerializable() throws Exception {
        boolean restricted = txBase(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, true, true);

        // Task must complete without exception.
        assert !restricted;
    }

    /**
     * Execution within explicit lock.
     *
     * @throws Exception if fails.
     */
    public void testExplicitLock() throws Exception {
        boolean restricted = explicitLockBase(false);

        // Task must abort with exception.
        assert restricted;
    }

    /**
     * Execution within explicit lock.
     *
     * @throws Exception if fails.
     */
    public void testExplicitLockMulti() throws Exception {
        boolean restricted = explicitLockBase(true);

        // Task must abort with exception.
        assert restricted;
    }

    /**
     * Case without pending topology update.
     *
     * @throws Exception if fails.
     */
    public void testWithoutPendingTopUpdate() throws Exception {
        try (Ignite node1 = startGrid(1)) {

            final IgniteCache<Object, Object> cache1 = node1.createCache(txCacheConf("cache1"));
            final IgniteCache<Object, Object> cache2 = node1.createCache(txCacheConf("cache2"));

            // fill with test values
            fillCaches(ImmutableList.of(cache1, cache2));
            //waitForDiscovery(node1, node0);
            // Transaction manager starts on node1.
            try (Transaction tx = node1.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {

                // put it on node0.
                cache1.put(12, "node1 value");
                // Get key from node0.
                cache1.get(2);

                // Execute compute task.
                cache2.clear();

                tx.commit();
            }
            catch (IgniteException e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Populate cache with test keys.
     *
     * @param caches caches to fill with test entries.
     */
    private void fillCaches(List<IgniteCache<Object, Object>> caches) {
        for (IgniteCache<Object, Object> cache : caches)
            for (int i = 0 ; i < 10; i++)
                cache.put(i, i);
    }

    /**
     * Test execution within transaction.
     *
     * @param concurrency transaction concurrency level.
     * @param isolation transaction isolation level.
     * @return true if compute task was aborted.
     * @throws Exception if fails.
     */
    private boolean txBase(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        return txBase(concurrency, isolation, false, true);
    }

    /**
     * Test execution within transaction.
     *
     * @param concurrency transaction concurrency level.
     * @param isolation transaction isolation level.
     * @param writeLock true if write lock required (otherwise only get will be invoked)
     * @param multipleInstances cluster mode
     * @return true if compute task was aborted.
     * @throws Exception if fails.
     */
    private boolean txBase(final TransactionConcurrency concurrency, final TransactionIsolation isolation, final boolean writeLock,
        final boolean multipleInstances) throws Exception {

        final Ignite node0 = startGrid(0);

        try (Ignite node1 = multipleInstances ? startGrid(1) : node0) {

            final IgniteCache<Object, Object> cache1 = node1.createCache(txCacheConf("cache1"));
            final IgniteCache<Object, Object> cache2 = node1.createCache(txCacheConf("cache2"));

            // fill with test values
            fillCaches(ImmutableList.of(cache1, cache2));

            if (multipleInstances)
                waitForDiscovery(node1, node0);

            final CountDownLatch sync = new CountDownLatch(1);

            IgniteInternalFuture<Boolean> fut = runAsync(new Callable<Boolean>() {
                @Override public Boolean call() throws IgniteInterruptedCheckedException {
                    // Transaction manager starts on node1.
                    try (Transaction tx = node1.transactions().txStart(concurrency, isolation)) {

                        if (writeLock)
                            // put it on node0.
                            cache1.put(12, "node1 value");
                        else
                            // Get key from node0.
                            cache1.get(2);

                        // Signal main thread.
                        sync.countDown();

                        // Await when new cache will be created.
                        assert GridTestUtils.waitForCondition(new PA() {
                            @Override public boolean apply() {
                                return node1.cacheNames().size() >= 3;
                            }
                        }, CACHE_CREATION_TIMEOUT);

                        // Execute compute task.
                        cache2.clear();

                        tx.commit();
                    }
                    catch (IgniteException e) {
                        return true;
                    }

                    return false;
                }
            });

            // create watch-thread that will interrupt task if timeout expired
            track(fut, PENDING_TIMEOUT);

            // await new thread
            sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

            // create cache - topology update
            node0.createCache("cache3");

            // awaiting task to be complete
            try {
                return fut.get();
            } catch (CancellationException e) {
                fail("Timeout expired - deadlock possible");
            }
        }
        finally {
            if (multipleInstances)
                node0.close();
        }
        return false;
    }

    /**
     * Test execution within lock.
     *
     * @param multipleInstances cluster mode
     * @return true if compute task was aborted.
     * @throws Exception if fails.
     */
    private boolean explicitLockBase(boolean multipleInstances) throws Exception {

        final Ignite node0 = startGrid(0);

        try (Ignite node1 = multipleInstances ? startGrid(1) : node0) {

            final IgniteCache<Object, Object> cache1 = node1.createCache(txCacheConf("cache1"));
            final IgniteCache<Object, Object> cache2 = node1.createCache(txCacheConf("cache2"));

            // fill with test values
            fillCaches(ImmutableList.of(cache1, cache2));

            if (multipleInstances)
                waitForDiscovery(node1, node0);

            final CountDownLatch sync = new CountDownLatch(1);

            IgniteInternalFuture<Boolean> fut = runAsync(new Callable<Boolean>() {
                @Override public Boolean call() throws IgniteInterruptedCheckedException {
                    Lock lock = cache1.lock(2);

                    lock.lock();

                    try {
                        // Signal main thread.
                        sync.countDown();

                        // Await when new cache will be created.
                        assert GridTestUtils.waitForCondition(new PA() {
                            @Override public boolean apply() {
                                return node1.cacheNames().size() >= 3;
                            }
                        }, CACHE_CREATION_TIMEOUT);

                        // Execute compute task.
                        cache2.clear();
                    }
                    catch (IgniteException e) {
                        return true;
                    }
                    finally {
                        lock.unlock();
                    }

                    return false;
                }
            });

            // Track future completion.
            track(fut, PENDING_TIMEOUT);

            // Await new thread.
            sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

            // Create cache - topology update.
            node0.createCache("cache3");

            // Await task to be complete.
            try {
                return fut.get();
            } catch (CancellationException e) {
                fail("Timeout expired - deadlock possible");
            }
        }
        finally {
            if (multipleInstances)
                node0.close();
        }
        return false;
    }

    /**
     * create tracking thread that will interrupt task if timeout expired
     * @param fut future to track
     * @param timeout timeout in millis
     * @return started watch thread
     */
    private Thread track(final IgniteInternalFuture<Boolean> fut, final long timeout) {
        Thread watchThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    // busy wait for completion
                    boolean done = GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return fut.isDone();
                        }
                    }, timeout);

                    if (!done)
                        fut.cancel();

                } catch (IgniteCheckedException ignore) {
                    // ignore and exit
                }
            }
        });

        watchThread.start();

        return watchThread;
    }

    /**
     * Affinity function ensures that "even" keys will be on "even" node and "odd" keys on "odd"
     */
    private static class TestAffinityFunction implements AffinityFunction {
        /** {@inheritDoc} */
        @Override public int partitions() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return ((Number)key).intValue() % 2;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(partitions());

            for (int part = 0; part < partitions(); part++)
                res.add(nodes(part, affCtx.currentTopologySnapshot()));

            return res;
        }

        @SuppressWarnings({"RedundantTypeArguments"})
        private List<ClusterNode> nodes(int part, Collection<ClusterNode> nodes) {
            Collection<ClusterNode> col = new HashSet<>(nodes);

            if (col.size() <= 1)
                return new ArrayList<>(col);

            for (Iterator<ClusterNode> iter = col.iterator(); iter.hasNext(); ) {
                ClusterNode node = iter.next();

                boolean even = node.<Boolean>attribute("EVEN");

                if (even && part % 2 != 0 || !even && part % 2 == 0)
                    iter.remove();
            }
            return new ArrayList<>(col);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }
}