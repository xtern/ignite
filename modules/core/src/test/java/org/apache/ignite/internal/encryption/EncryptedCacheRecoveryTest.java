package org.apache.ignite.internal.encryption;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Before;
import org.junit.Test;

public class EncryptedCacheRecoveryTest extends AbstractEncryptionTest {
    @Before
    public void setup() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected int partitions() {
        return 4;
    }

    @Test
    public void checkWalRecovery() throws Exception {
        checkRecovery(false);
    }

    @Test
    public void checkRecovery() throws Exception {
        checkRecovery(true);
    }

    private void checkRecovery(boolean cp) throws Exception {
        IgniteEx ignite = startGrid(0);
        ignite.cluster().state(ClusterState.ACTIVE);

        ((GridCacheDatabaseSharedManager)ignite.context().cache().context().database()).enableCheckpoints(cp).get();

        createEncryptedCache(ignite, null, cacheName(), null);

        if (cp)
            forceCheckpoint();

        stopGrid(0, true);

        System.out.println(">> start grid (with recovery)");

        ignite = startGrid(0);
        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Long, Object> cache = ignite.cache(cacheName());

        for (long i = 0; i < 104; i++)
            assertEquals("" + i, cache.get(i));

//            if (pages != null) {
//                List<Integer> curPags = allocatedPages(ignite, CACHE2_NAME);
//
//                assertEquals(pages, curPags);
//            }



//            for (int i = 0; i < 128; i++)
//                cache.put((long)cnt.incrementAndGet(), "" + cnt.incrementAndGet());

//            stopGrid(0, true);
//        }
    }
}
