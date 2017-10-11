package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
/** */
public class CacheMetricsTxAvgTimeTest extends GridCommonAbstractTest {
    /** */
    private <K, V> CacheConfiguration<K, V> cacheConfiguration(String name) {
        CacheConfiguration<K, V> cacheConfiguration = new CacheConfiguration<>(name);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheConfiguration.setStatisticsEnabled(true);
        return cacheConfiguration;
    }

    /** */
    public void testTxCommitDuration() throws Exception {
        try ( Ignite node = startGrid(0)) {
            IgniteCache<Object, Object> cache = node.createCache(cacheConfiguration(DEFAULT_CACHE_NAME));

            try (Transaction tx = node.transactions().txStart()) {
                cache.put(1, 1);

                // Await 1 second.
                U.sleep(1_000);

                tx.commit();
            }

            // Documentation says that this metric is in microseconds.
            float commitTime = cache.metrics().getAverageTxCommitTime();

            // But this assertion will fail because it in milliseconds and returns only ~1000.
            assert commitTime >= 1_000_000;
        }
    }
}
