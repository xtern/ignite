package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class PutAllTxCheck extends GridCommonAbstractTest {



    @Test
    public void check() throws Exception {
        Ignite node = startGrids(2);

        IgniteCache cache = node.createCache(ccfg(DEFAULT_CACHE_NAME));

//        Map<Integer, Integer> data = new HashMap<>();

        for (int i = 0; i < 3; i++)
            cache.put(i, i);

//        cache.putAll(data);
    }

    private CacheConfiguration ccfg(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
//        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setBackups(1);

        return ccfg;
    }
}
