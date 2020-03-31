package org.apache.ignite.internal.encryption;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CACHE_KEY_CHANGE_PREPARE;
import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.DEFAULT_MASTER_KEY_NAME;

/**
 *
 */
public class CacheKeyChangeTest extends AbstractEncryptionTest {
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

    @Override protected int partitions() {
        return 4;
    }

    @Override protected CacheMode cacheMode() {
        return CacheMode.REPLICATED;
    }

    @Test
    public void checkReencryption() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        IgniteEx node1 = grids.get1();

        IgniteEx node2 = grids.get2();

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint(node1);
        forceCheckpoint(node2);

        node1.context().encryption().reencrypt(cacheName());

        //node1.cache()
    }

    @Test
    public void distributedKeyChange() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        IgniteEx node1 = grids.get1();

        IgniteEx node2 = grids.get2();

        createEncryptedCache(node1, node2, cacheName(), null);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        CacheKeyChangeRequest req1 = new CacheKeyChangeRequest(node1.cluster().localNode().id());

        CacheKeyChangeRequest req2 = new CacheKeyChangeRequest(node2.cluster().localNode().id());

        Function<CacheKeyChangeRequest, IgniteInternalFuture<CacheKeyChangeResponse>> nodeFunc = (rq) -> {
            GridFutureAdapter<CacheKeyChangeResponse> fut = new GridFutureAdapter<>();

            log.info("Create response: " + rq.reqId);

            CacheKeyChangeResponse res = new CacheKeyChangeResponse(rq.reqId());

            fut.onDone(res);

            return fut;
        };

        DistributedProcess<CacheKeyChangeRequest, CacheKeyChangeResponse> ps1 =
            new DistributedProcess<>(node1.context(), CACHE_KEY_CHANGE_PREPARE, nodeFunc, this::finishPrepareCacheKeyChange);

        DistributedProcess<CacheKeyChangeRequest, CacheKeyChangeResponse> ps2 =
            new DistributedProcess<>(node2.context(), CACHE_KEY_CHANGE_PREPARE, nodeFunc, this::finishPrepareCacheKeyChange);

        ps1.start(req1.reqId(), req1);

//        ps2.start(req2.reqId(), req2);

    }

    private void finishPrepareCacheKeyChange(UUID id, Map<UUID, CacheKeyChangeResponse> res, Map<UUID, Exception> err) {
//        if (!err.isEmpty()) {
//            if (masterKeyChangeRequest != null && masterKeyChangeRequest.requestId().equals(id))
//                masterKeyChangeRequest = null;
//
//            completeMasterKeyChangeFuture(id, err);
//        }
//        else if (isCoordinator())
//            performMKChangeProc.start(id, masterKeyChangeRequest);

        log.info(">xxx> finish prep [id=" + id + ", res=" + res + "]");
    }

    static class CacheKeyChangeRequest implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Request id. */
        private final UUID reqId;

        CacheKeyChangeRequest(UUID reqId) {
            this.reqId = reqId;
        }

        public UUID reqId() {
            return reqId;
        }
    }

    static class CacheKeyChangeResponse implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Request id. */
        private final UUID resId;

        CacheKeyChangeResponse(UUID resId) {
            this.resId = resId;
        }

        public UUID getResId() {
            return resId;
        }
    }
}
