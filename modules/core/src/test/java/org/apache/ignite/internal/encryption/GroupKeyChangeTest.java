package org.apache.ignite.internal.encryption;

import java.util.Collections;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.junit.Test;

/**
 *
 */
public class GroupKeyChangeTest extends AbstractEncryptionTest {
    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

//    /** {@inheritDoc} */
//    @Override protected void beforeTest() throws Exception {
//        cleanPersistenceDir();
//
//        IgniteEx igniteEx = startGrid(0);
//
//        startGrid(1);
//
//        igniteEx.cluster().active(true);
//
//        awaitPartitionMapExchange();
//    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration memCfg = cfg.getDataStorageConfiguration();

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(10L * 1024 * 1024)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int partitions() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.REPLICATED;
    }

    @Test
    public void checkCacheStart() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);
    }

    @Test
    public void checkDistribProcess() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        IgniteInternalCache<Object, Object> cache = node1.cachex(cacheName());

        int grpId = cache.context().groupId();

        node1.encryption().changeGroupKey(Collections.singletonList(grpId));

        System.out.println("change finished");

        cache.put(-1000, "-1000");

        forceCheckpoint();

        stopAllGrids();

        node1 = startGrid(GRID_0);
        startGrid(GRID_1);

        node1.cluster().state(ClusterState.ACTIVE);

        checkData(node1);

        cache = node1.cachex(cacheName());

        cache.put(-2000, "-2000");

        assertEquals("-1000", cache.get(-1000));
        assertEquals("-2000", cache.get(-2000));

        try (IgniteDataStreamer streamer = node1.dataStreamer(cacheName())) {
            for (int i = 1000; i < 50_000; i++)
                streamer.addData(i, "" + i);
        }

        stopAllGrids();

        node1 = startGrid(GRID_0);
        node2 = startGrid(GRID_1);

        node1.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        IgniteCache<Integer, String> cache0 = node1.cache(cacheName());

        assert cache0 != null;

        System.out.println("cache size = " + cache0.size());

        Iterator<Cache.Entry<Integer, String>> itr = cache0.iterator();

        assert itr.hasNext();

        int cntr = 0;

        while (itr.hasNext()) {
            Cache.Entry<Integer, String> e = itr.next();

            assertEquals("" + e.getKey(), e.getValue());

            cntr++;
        }

        assertEquals(cache0.size(), cntr);
//        for (int i = 1000; i < 50_000; i++)
//            cache.put(i, UUID.randomUUID());
    }
}
