package org.apache.ignite.internal.encryption;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ACTIVE_KEY_ID_FOR_GROUP;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class GroupKeyChangeTest extends AbstractEncryptionTest {
    private static final long MAX_AWAIT_MILLIS = 15_000;

    private DiscoveryHook discoveryHook;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (discoveryHook != null)
            ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(discoveryHook);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(10L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalSegmentSize(1024 * 1024)
            .setWalSegments(4)
            .setMaxWalArchiveSize(10 * 1024 * 1024)
            .setWalMode(LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, String grp) {
        CacheConfiguration<K, V> cfg = super.cacheConfiguration(name, grp);

        return cfg.setAffinity(new RendezvousAffinityFunction(false, 8));
    }

//    @Test
//    public void testNodeFailsBeforePrepare() throws Exception {
//        CountDownLatch discoLatch = new CountDownLatch(1);
//
//        discoveryHook = new InitMessageDiscoHook(discoLatch, DistributedProcessType.GROUP_KEY_CHANGE_FINISH);
//
//        IgniteEx grid0 = startGrid(GRID_0);
//
//        discoveryHook = null;
//
//        IgniteEx grid1 = startGrid(GRID_1);
//
//        grid0.cluster().active(true);
//
//        createEncryptedCache(grid0, grid1, cacheName(), null);
//
//        int grpId = CU.cacheId(cacheName());
//
//        IgniteFuture fut = grid1.encryption().changeGroupKey(Collections.singleton(grpId));
//
//        stopGrid(GRID_0);
//
//        discoLatch.countDown();
//
//        fut.get();
//
//        startGrid(GRID_0);
//    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectNodeJoinDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        assertEquals(0, grids.get1().context().encryption().groupKey(grpId).id());

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeGroupKey(Collections.singleton(grpId));

        commSpi.waitForBlocked();

        assertThrowsWithCause(() -> startGrid(3), IgniteCheckedException.class);

        commSpi.stopBlock();

        fut.get();

        checkGroupKey(grpId, 1);

        checkEncryptedCaches(grids.get1(), grids.get2());
    }

    @Test
    public void testRejectWhenNotAllBltNodesPresent() throws Exception {
        startTestGrids(true);

        createEncryptedCache(grid(GRID_0), grid(GRID_1), cacheName(), null);

        stopGrid(GRID_1);

        assertThrowsAnyCause(log, () -> {
            return grid(GRID_0).encryption().changeGroupKey(Collections.singleton(CU.cacheId(cacheName())));
        }, IgniteException.class, "Not all baseline nodes online [total=2, online=1]");
    }

    @Test
    public void testEncryptionRecoveryFromWal() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        enableCheckpoints(node0, false);
        enableCheckpoints(node1, false);

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeGroupKey(Collections.singleton(grpId)).get();

        node0.context().encryption().encryptionTask(grpId).get();
        node1.context().encryption().encryptionTask(grpId).get();

        assertEquals(1, node0.context().encryption().groupKey(grpId).id());
        assertEquals(1, node1.context().encryption().groupKey(grpId).id());

        stopAllGrids();

        System.out.println(">>> Start grid");

        nodes = startTestGrids(false);

        node0 = nodes.get1();
        node1 = nodes.get2();

        enableCheckpoints(node0, true);
        enableCheckpoints(node1, true);

        awaitPartitionMapExchange();

        checkGroupKey(grpId, 1);
    }

    @Test
    public void testNodeFailsBeforePrepare() throws Exception {
        checkNodeFailsDuringRotation(false, true, true);
    }

    @Test
    public void testCrdFailsBeforePrepare() throws Exception {
        checkNodeFailsDuringRotation(true, true, true);
    }

    @Test
    public void testNodeFailsBeforePerform() throws Exception {
        checkNodeFailsDuringRotation(false, false, true);
    }

    @Test
    public void testCrdFailsBeforePerform() throws Exception {
        checkNodeFailsDuringRotation(true, false, true);
    }

    @Test
    public void testNodeFailsAfterPrepare() throws Exception {
        checkNodeFailsDuringRotation(false, true, false);
    }

    @Test
    public void testCrdFailsAfterPrepare() throws Exception {
        checkNodeFailsDuringRotation(true, true, false);
    }

    @Test
    public void testNodeFailsAfterPerform() throws Exception {
        checkNodeFailsDuringRotation(false, false, false);
    }

    @Test
    public void testCrdFailsAfterPerform() throws Exception {
        checkNodeFailsDuringRotation(true, false, false);
    }

    /**
     * @param stopCrd {@code True} if stop coordinator.
     * @param prepare {@code True} if stop on the prepare phase. {@code False} if stop on the perform phase.
     */
    private void checkNodeFailsDuringRotation(boolean stopCrd, boolean prepare, boolean discoBlock) throws Exception {
        CountDownLatch discoLatch = new CountDownLatch(discoBlock ? 1 : 0);

        DistributedProcessType type = prepare ?
            DistributedProcessType.GROUP_KEY_CHANGE_PREPARE : DistributedProcessType.GROUP_KEY_CHANGE_FINISH;

        if (stopCrd)
            discoveryHook = new InitMessageDiscoHook(discoLatch, type);

        IgniteEx grid0 = startGrid(GRID_0);

        if (!stopCrd)
            discoveryHook = stopCrd ? null : new InitMessageDiscoHook(discoLatch, type);

        IgniteEx grid1 = startGrid(GRID_1);

        grid0.cluster().active(true);

        createEncryptedCache(grid0, grid1, cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        int keyId = 0;

        checkGroupKey(grpId, keyId);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid1);

        if (!discoBlock) {
            AtomicBoolean preparePhase = new AtomicBoolean(true);

            spi.blockMessages((node, msg) -> {
                if (msg instanceof SingleNodeMessage) {
                    boolean isPrepare = preparePhase.compareAndSet(true, false);

                    return prepare || !isPrepare;
                }

                return false;
            });
        }

        String alive = stopCrd ? GRID_1 : GRID_0;
        String stopped = stopCrd ? GRID_0 : GRID_1;

        IgniteFuture<Void> fut = grid(alive).encryption().changeGroupKey(Collections.singleton(grpId));

        IgniteInternalFuture stopFut;

        if (!discoBlock) {
            spi.waitForBlocked();

            stopFut = runAsync(() -> {
                if (stopCrd)
                    stopGrid(GRID_0, true);
                else
                    stopGrid(GRID_1, true);
            });
        }
        else {
            stopFut = new GridFinishedFuture();

            if (stopCrd)
                stopGrid(GRID_0, true);
            else
                stopGrid(GRID_1, true);

            discoLatch.countDown();
        }

        if (prepare)
            assertThrowsAnyCause(log, fut::get, IgniteCheckedException.class, null);
        else {
            keyId++;

            fut.get();
        }

        stopFut.get();

        checkGroupKey(grpId, keyId);

        if (prepare) {
            IgniteEx stoppedNode = startGrid(stopped);

            stoppedNode.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

            awaitPartitionMapExchange();

            stoppedNode.encryption().changeGroupKey(Collections.singleton(grpId)).get(MAX_AWAIT_MILLIS);

            checkGroupKey(grpId, keyId + 1);
        }
        else {
            System.setProperty(IGNITE_ACTIVE_KEY_ID_FOR_GROUP + grpId, String.valueOf(keyId));

            try {
                IgniteEx stoppedNode = startGrid(stopped);

                stoppedNode.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

                awaitPartitionMapExchange();

                stoppedNode.encryption().changeGroupKey(Collections.singleton(grpId)).get(MAX_AWAIT_MILLIS);

                checkGroupKey(grpId, keyId + 1);
            } finally {
                System.clearProperty(IGNITE_ACTIVE_KEY_ID_FOR_GROUP + grpId);
            }
        }
    }

    @Test
    public void testBasicChangeUnderLoad() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        IgniteInternalCache<Object, Object> cache = node1.cachex(cacheName());

        AtomicInteger cntr = new AtomicInteger(cache.size());

        CountDownLatch startLatch = new CountDownLatch(1);

        final Ignite somenode = node1;

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            try (IgniteDataStreamer<Integer, String> streamer = somenode.dataStreamer(cacheName())) {
                while (!Thread.currentThread().isInterrupted()) {
                    int n = cntr.getAndIncrement();

                    streamer.addData(n, String.valueOf(n));

                    if (n == 5000)
                        startLatch.countDown();
                }
            }
        });

        startLatch.await(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS);

        int grpId = cache.context().groupId();

        node1.encryption().changeGroupKey(Collections.singletonList(grpId)).get();

        Map<Integer, Integer> keys1 = node1.context().encryption().groupKeysInfo(grpId);
        Map<Integer, Integer> keys2 = node2.context().encryption().groupKeysInfo(grpId);

        assertEquals(2, keys1.size());
        assertEquals(2, keys2.size());

        assertEquals(keys1, keys2);

        info("New key was set on all nodes [grpId=" + grpId + ", keys=" + keys1 + "]");

        node1.context().encryption().encryptionTask(grpId).get(MAX_AWAIT_MILLIS);
        node2.context().encryption().encryptionTask(grpId).get(MAX_AWAIT_MILLIS);

        loadFut.cancel();

        info("Re-encryption finished");

        forceCheckpoint();

        // Ensure that data is encrypted with the new key.
        validateKeyIdentifier(node1.cachex(cacheName()).context().group(), 1);
        validateKeyIdentifier(node2.cachex(cacheName()).context().group(), 1);

        stopAllGrids();

        node1 = startGrid(GRID_0);
        node2 = startGrid(GRID_1);

        node1.cluster().state(ClusterState.ACTIVE);

        GridEncryptionManager encMgr1 = node1.context().encryption();
        GridEncryptionManager encMgr2 = node2.context().encryption();

        try (IgniteDataStreamer<Integer, String> streamer = node1.dataStreamer(cacheName())) {
            for (; ; ) {
                int n = cntr.getAndIncrement();

                streamer.addData(n, String.valueOf(n));

                if (n % 1000 == 0 && encMgr1.groupKeysInfo(grpId).size() == 1 && encMgr2.groupKeysInfo(grpId).size() == 1)
                    break;

                if (n > 10_000_000)
                    break;
            }
        }

        assertEquals(1, node1.context().encryption().groupKeysInfo(grpId).size());
        assertEquals(1, node2.context().encryption().groupKeysInfo(grpId).size());
    }

    @Test
    public void testBasicChange() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        IgniteInternalCache<Object, Object> cache = node1.cachex(cacheName());

        int grpId = cache.context().groupId();

        node1.encryption().changeGroupKey(Collections.singletonList(grpId)).get();

        Map<Integer, Integer> keys1 = node1.context().encryption().groupKeysInfo(grpId);
        Map<Integer, Integer> keys2 = node2.context().encryption().groupKeysInfo(grpId);

        assertEquals(2, keys1.size());
        assertEquals(2, keys2.size());

        assertEquals(keys1, keys2);

        info("New key was set on all nodes [grpId=" + grpId + ", keys=" + keys1 + "]");

        node1.context().encryption().encryptionTask(grpId).get(MAX_AWAIT_MILLIS);
        node2.context().encryption().encryptionTask(grpId).get(MAX_AWAIT_MILLIS);

        info("Re-encryption finished");

        forceCheckpoint();

        // Ensure that data is encrypted with the new key.
        validateKeyIdentifier(node1.cachex(cacheName()).context().group(), 1);
        validateKeyIdentifier(node2.cachex(cacheName()).context().group(), 1);

        stopAllGrids();

        node1 = startGrid(GRID_0);
        node2 = startGrid(GRID_1);

        node1.cluster().state(ClusterState.ACTIVE);

        GridEncryptionManager encMgr1 = node1.context().encryption();
        GridEncryptionManager encMgr2 = node2.context().encryption();

        try (IgniteDataStreamer<Integer, String> streamer = node1.dataStreamer(cacheName())) {
            for (int i = 1000; i < 500_000; i++) {
                streamer.addData(i, String.valueOf(i));

                if (i % 1000 == 0 && encMgr1.groupKeysInfo(grpId).size() == 1 && encMgr2.groupKeysInfo(grpId).size() == 1)
                    break;
            }
        }

        assertEquals(1, node1.context().encryption().groupKeysInfo(grpId).size());
        assertEquals(1, node2.context().encryption().groupKeysInfo(grpId).size());
    }

    private void validateKeyIdentifier(CacheGroupContext grp, int keyId) throws IgniteCheckedException, IOException {
        int grpId = grp.groupId();

        int realPageSize = grp.dataRegion().pageMemory().realPageSize(grpId);

        int encryptionBlockSize = grp.shared().kernalContext().config().getEncryptionSpi().blockSize();

        List<Integer> parts = IntStream.range(0, grp.shared().affinity().affinity(grpId).partitions())
            .boxed().collect(Collectors.toList());

        parts.add(INDEX_PARTITION);

        for (int p : parts) {
            FilePageStore pageStore =
                (FilePageStore)((FilePageStoreManager)grp.shared().pageStore()).getStore(grpId, p);

            if (!pageStore.exists())
                continue;

//            assertEquals("p=" + p, pageStore.encryptedPagesCount(), pageStore.encryptedPagesOffset());

//            assertEquals(0, pageStore.encryptedPagesCount());
//            assertEquals(0, pageStore.encryptedPagesOffset());

            long metaPageId = PageIdUtils.pageId(p, PageIdAllocator.FLAG_DATA, 0);

            scanFileStore(pageStore, metaPageId, realPageSize, encryptionBlockSize, keyId);
        }
    }

    private void scanFileStore(FilePageStore pageStore, long startPageId, int realPageSize, int blockSize, int expKeyIdentifier) throws IOException {
        int pagesCnt = pageStore.pages();
        int pageSize = pageStore.getPageSize();

        ByteBuffer pageBuf = ByteBuffer.allocate(pageSize);

        try (FileChannel ch = FileChannel.open( new File(pageStore.getFileAbsolutePath()).toPath(), StandardOpenOption.READ)) {
            for (int n = 0; n < pagesCnt; n++) {
                long pageId = startPageId + n;

                long pageOffset = pageStore.pageOffset(pageId);

                pageBuf.position(0);

                ch.position(pageOffset);
                ch.read(pageBuf);

                pageBuf.position(realPageSize + blockSize + 4);

                assertEquals(expKeyIdentifier, pageBuf.get() & 0xff);
            }
        }
    }

    private void checkGroupKey(int grpId, int keyId) throws IgniteCheckedException, IOException {
        for (Ignite g : G.allGrids()) {
            IgniteEx grid = (IgniteEx)g;

            if (grid.context().clientNode())
                continue;

            GridEncryptionManager encrMgr = grid.context().encryption();

            assertEquals(grid.localNode().id().toString(), keyId, encrMgr.groupKey(grpId).id());

            encrMgr.encryptionTask(grpId).get();

            // todo check after restart without checkpoint.
            forceCheckpoint(g);

            info("Validating page store [node=" + g.cluster().localNode().id() + ", grp=" + grpId + "]");

            validateKeyIdentifier(grid.context().cache().cacheGroup(grpId), keyId);
        }
    }

    private static class InitMessageDiscoHook extends DiscoveryHook {
        private final CountDownLatch discoLatch;
        private final DistributedProcessType type;

        private InitMessageDiscoHook(CountDownLatch discoLatch, DistributedProcessType type) {
            this.discoLatch = discoLatch;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
            if (customMsg instanceof InitMessage) {
                InitMessage msg = (InitMessage)customMsg;

                if (msg.type() == type.ordinal()) {
                    try {
                        discoLatch.await(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
