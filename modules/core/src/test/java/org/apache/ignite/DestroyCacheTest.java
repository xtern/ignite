package org.apache.ignite;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;


public class DestroyCacheTest extends GridCommonAbstractTest {

    private CacheConfiguration<Integer, Boolean> ccfg(String name, String grp) {
        return new CacheConfiguration<Integer, Boolean>(name).setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setGroupName(grp);
    }

    public void testDestroyAsync() throws Exception {
        String grpName = "testGroup";

        try (IgniteEx node = startGrid(0)) {
            node.createCache(ccfg("cache2", grpName));

            for (int n = 0; n < 100; n++) {
                IgniteCache<Integer, Boolean> cache1 = node.createCache(ccfg("cache1", grpName));

                AtomicInteger cntr = new AtomicInteger();

                GridTestUtils.runMultiThreadedAsync(() -> {
                    try {
                        int key;

                        while ((key = cntr.getAndIncrement()) < 10_000) {
                            if (key == 1000)
                                cache1.destroy();

                            cache1.putIfAbsent(key, true);
                        }
                    }
                    catch (Exception ignore) {
                        log.warning(ignore.getMessage());
                    }

                    return null;
                }, 6, "put-thread").get();
            }

//            System.out.println("HANGED THREADS: " + acq.size());
//
//            for (Thread t : acq.values())
//                U.dumpStack(t);
        }
    }
}
