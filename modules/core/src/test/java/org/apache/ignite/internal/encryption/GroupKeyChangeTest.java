package org.apache.ignite.internal.encryption;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.GROUP_KEY_CHANGE_PREPARE;
import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.DEFAULT_MASTER_KEY_NAME;

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
    }

    @Test
    public void checkReencryption2() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        node1.cluster().state(ClusterState.ACTIVE_READ_ONLY);

        EncryptionSpi spi = node1.context().config().getEncryptionSpi();

        byte[] key = node1.context().config().getEncryptionSpi().encryptKey(spi.create());

        node1.context().encryption().rescan(cacheName(), key);
        node2.context().encryption().rescan(cacheName(), key);

        forceCheckpoint();

        stopAllGrids(false);

        startTestGrids(false);

        node1 = grid(GRID_0);
        node2 = grid(GRID_1);

        IgniteCache<Object, Object> cache0 = node1.cache(ENCRYPTED_CACHE);

        assert cache0 != null;

        for (long i = 0; i < 104; i++)
            assertEquals("" + i, cache0.get(i));
    }

    @Test
    public void checkReencryption() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        node1.cluster().state(ClusterState.ACTIVE_READ_ONLY);

        EncryptionSpi spi = node1.context().config().getEncryptionSpi();

        byte[] key = node1.context().config().getEncryptionSpi().encryptKey(spi.create());

        node1.context().encryption().reencrypt(cacheName(), key);
        node2.context().encryption().reencrypt(cacheName(), key);

        forceCheckpoint();

        stopAllGrids(false);

        startTestGrids(false);

        node1 = grid(GRID_0);
        node2 = grid(GRID_1);

        IgniteCache<Object, Object> cache0 = node1.cache(ENCRYPTED_CACHE);

        assert cache0 != null;

        for (long i = 0; i < 104; i++)
            assertEquals("" + i, cache0.get(i));
    }

    @Test
    public void checkReencryptionInactive() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        node1.cluster().state(ClusterState.INACTIVE);

        EncryptionSpi spi = node1.context().config().getEncryptionSpi();

        byte[] key = node1.context().config().getEncryptionSpi().encryptKey(spi.create());

        node1.context().encryption().reencryptInactive(cacheName(), key);
        node2.context().encryption().reencryptInactive(cacheName(), key);

//        forceCheckpoint();

        node1.cluster().state(ClusterState.ACTIVE);

        //stopAllGrids(false);

//        startTestGrids(false);
//
//        node1 = grid(GRID_0);
//        node2 = grid(GRID_1);
//
        IgniteCache<Object, Object> cache0 = node1.cache(cacheName());
//
//        assert cache0 != null;
//
        for (long i = 0; i < 104; i++)
            assertEquals("" + i, cache0.get(i));
    }
}
