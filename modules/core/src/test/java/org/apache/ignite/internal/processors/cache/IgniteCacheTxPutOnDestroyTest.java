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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Test ensures that the put operation does not hang during asynchronous cache destroy.
 */
public abstract class IgniteCacheTxPutOnDestroyTest extends IgniteCachePutOnDestroyTest {
    /** {@inheritDoc} */
    @Override public CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCacheOptimisticReadCommitted() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCacheOptimisticRepeatableRead() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCacheOptimisticSerializable() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCachePessimisticReadCommitted() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCachePessimisticRepeatableRead() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCachePessimisticSerializable() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestTxPutOnDestroyCache0(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws IgniteCheckedException {
        for (int n = 0; n < ITER_CNT; n++)
            doTestTxPutOnDestroyCache(concurrency, isolation);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestTxPutOnDestroyCache(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws IgniteCheckedException {

        final Ignite ignite = grid(0);

        final String grpName = "testGroup";

        IgniteCache additionalCache = ignite.createCache(cacheConfiguration("cache1", grpName));

        try {
            IgniteCache<Integer, Boolean> cache = ignite.getOrCreateCache(cacheConfiguration("cache2", grpName));

            AtomicInteger cntr = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    int key;

                    while ((key = cntr.getAndIncrement()) < 2_000) {
                        if (key == 1_000) {
                            cache.destroy();

                            break;
                        }

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.put(key, true);

                            tx.commit();
                        }
                    }
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), hasCacheStoppedMessage(e));
                }

                return null;
            }, 6, "put-thread").get(TIMEOUT);
        }
        finally {
            additionalCache.destroy();
        }
    }
}
