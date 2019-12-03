package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheRebalancingAbstractTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;

public class IndexedCacheFileRebalanceTest extends GridCommonAbstractTest {
    /** Cache with enabled indexes. */
    private static final String INDEXED_CACHE = "indexed";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setRebalanceThreadPoolSize(2);

//        CacheConfiguration ccfg1 = cacheConfiguration(CACHE)
//            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
//            .setBackups(2)
//            .setRebalanceMode(CacheRebalanceMode.ASYNC)
//            .setIndexedTypes(Integer.class, Integer.class)
//            .setAffinity(new RendezvousAffinityFunction(false, 32))
//            .setRebalanceBatchesPrefetchCount(2)
//            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        CacheConfiguration ccfg2 = cacheConfiguration(INDEXED_CACHE)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qryEntity = new QueryEntity(Integer.class.getName(), TestValue.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("v1", Integer.class.getName());
        fields.put("v2", Integer.class.getName());

        qryEntity.setFields(fields);

        QueryIndex qryIdx = new QueryIndex("v1", true);

        qryEntity.setIndexes(Collections.singleton(qryIdx));

        ccfg2.setQueryEntities(Collections.singleton(qryEntity));

        CacheConfiguration[] cacheCfgs = new CacheConfiguration[1];
        cacheCfgs[0] = ccfg2;

//        if (filteredCacheEnabled && !gridName.endsWith("0")) {
//            CacheConfiguration ccfg3 = cacheConfiguration(FILTERED_CACHE)
//                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
//                .setBackups(2)
//                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
//                .setNodeFilter(new IgnitePdsCacheRebalancingAbstractTest.CoordinatorNodeFilter());
//
//            cacheCfgs.add(ccfg3);
//        }

        cfg.setCacheConfiguration(cacheCfgs);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
            .setCheckpointFrequency(3_000)
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(1024)
            .setWalSegmentSize(8 * 1024 * 1024) // For faster node restarts with enabled persistence.
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("dfltDataRegion")
                .setPersistenceEnabled(true)
                .setMaxSize(512 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    protected CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setBackups(1);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void checkSimpleRebalancing() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        final ConcurrentMap<Integer, TestValue> map = new ConcurrentHashMap<>();

        try (IgniteDataStreamer<Integer, TestValue> ds = node0.dataStreamer(INDEXED_CACHE)) {
            for (int i = 0; i < 10_000; i++) {
                ds.addData(i, new TestValue(i, i, i));
                map.put(i, new TestValue(i, i, i));
            }
        }

        forceCheckpoint();

        startGrid(1);

        node0.cluster().setBaselineTopology(2);

        awaitPartitionMapExchange();

        for (int i = 10_000; i < 11_000; i++)
            node0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));
    }

    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void checkIndexEvict() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        IgniteInternalCache cache = node0.cachex(INDEXED_CACHE);

        CacheGroupContext grp = cache.context().group();

        GridCacheSharedContext<Object, Object> cctx = node0.context().cache().context();

        node0.context().cache().context().database().checkpointReadLock();

        try {
            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

            ((FilePageStoreManager)cctx.pageStore()).getStore(grp.groupId(), PageIdAllocator.INDEX_PARTITION).truncate(tag);
        } finally {
            node0.context().cache().context().database().checkpointReadUnlock();
        }

        assert !cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        cache.context().offheap().start(cctx, grp);

        assert cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        //qryProc.rebuildIndexesFromHash(ctx)

        final ConcurrentMap<Integer, TestValue> map = new ConcurrentHashMap<>();

        try (IgniteDataStreamer<Integer, TestValue> ds = node0.dataStreamer(INDEXED_CACHE)) {
            for (int i = 0; i < 10_000; i++) {
                ds.addData(i, new TestValue(i, i, i));
                map.put(i, new TestValue(i, i, i));
            }
        }

//        forceCheckpoint();
//
//        startGrid(1);
//
//        node0.cluster().setBaselineTopology(2);
//
//        awaitPartitionMapExchange();
//
//        for (int i = 10_000; i < 11_000; i++)
//            node0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));
    }

    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void checkIndexEvictRebuild() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        IgniteInternalCache cache = node0.cachex(INDEXED_CACHE);

        CacheGroupContext grp = cache.context().group();

        GridCacheSharedContext<Object, Object> cctx = node0.context().cache().context();

        try (IgniteDataStreamer<Integer, TestValue> ds = node0.dataStreamer(INDEXED_CACHE)) {
            for (int i = 0; i < 10_000; i++)
                ds.addData(i, new TestValue(i, i, i));
        }

        U.sleep(1_000);

        node0.context().cache().context().database().checkpointReadLock();

        try {
            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

            ((FilePageStoreManager)cctx.pageStore()).getStore(grp.groupId(), PageIdAllocator.INDEX_PARTITION).truncate(tag);
        } finally {
            node0.context().cache().context().database().checkpointReadUnlock();
        }

        assert !cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        log.info(">>>>> start");

        GridQueryProcessor qryProc = cctx.kernalContext().query();

        GridCacheContextInfo cacheInfo = new GridCacheContextInfo(cache.context(), false);

        cache.context().offheap().start(cctx, grp);

        qryProc.onCacheStop0(cacheInfo, false);
        qryProc.onCacheStart0(cacheInfo, node0.context().cache().cacheDescriptor(INDEXED_CACHE).schema(), node0.context().cache().cacheDescriptor(INDEXED_CACHE).sql());

//
//
//        cctx.kernalContext().query().onCacheStart(new GridCacheContextInfo(cache.context(), false),
//            );

        assert cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        log.info(">>>>> started");

        assert qryProc.moduleEnabled();

        qryProc.rebuildIndexesFromHash(cache.context()).get();


        cache.put(100_000, new TestValue(100_000, 100_000, 100_000));

//        forceCheckpoint();
//
//        startGrid(1);
//
//        node0.cluster().setBaselineTopology(2);
//
//        awaitPartitionMapExchange();
//
//        for (int i = 10_000; i < 11_000; i++)
//            node0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** Operation order. */
        private final long order;

        /** V 1. */
        private final int v1;

        /** V 2. */
        private final int v2;

        /** Flag indicates that value has removed. */
        private final boolean removed;

        private TestValue(long order, int v1, int v2) {
            this(order, v1, v2, false);
        }

        private TestValue(long order, int v1, int v2, boolean removed) {
            this.order = order;
            this.v1 = v1;
            this.v2 = v2;
            this.removed = removed;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TestValue testValue = (TestValue) o;

            return order == testValue.order &&
                v1 == testValue.v1 &&
                v2 == testValue.v2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(order, v1, v2);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestValue{" +
                "order=" + order +
                ", v1=" + v1 +
                ", v2=" + v2 +
                '}';
        }
    }


}
