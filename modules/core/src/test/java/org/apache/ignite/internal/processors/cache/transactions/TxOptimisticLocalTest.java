package org.apache.ignite.internal.processors.cache.transactions;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/** */
public class TxOptimisticLocalTest extends GridCommonAbstractTest {
    /** */
    public void testHanging() throws Exception {
        try (Ignite node = startGrid(0)) {
            for (int n = 0; n < 100; n++) {
                IgniteCache<Integer, Integer> cache = createCache(CacheMode.LOCAL, CacheWriteSynchronizationMode.FULL_SYNC);

                tryLocks(node, cache, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);

                cache.destroy();
            }
        }
    }

    /** */
    private void tryLocks(final Ignite ignite, final IgniteCache<Integer, Integer> cache,
        final TransactionConcurrency concurrency, final TransactionIsolation isolation) throws Exception {
        final AtomicInteger cnt = new AtomicInteger();

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final List<Integer> keys = ImmutableList.of(1, 2);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int threadNum = cnt.getAndIncrement();

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation, 500, 0)) {
                    cache.put(keys.get(threadNum), 0);
                    cache.put(keys.get(1 - threadNum), 0);

                    U.awaitQuiet(barrier);

                    tx.commit();
                }
            }
        }, 2, "tx-thread");
    }

    /** */
    private <K, V> IgniteCache<K, V> createCache(CacheMode cacheMode, CacheWriteSynchronizationMode syncMode) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(0);

        return ignite(0).getOrCreateCache(ccfg);
    }
}
