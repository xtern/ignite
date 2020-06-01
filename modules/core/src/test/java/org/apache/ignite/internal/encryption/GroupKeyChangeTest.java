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
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class GroupKeyChangeTest extends AbstractEncryptionTest {
    private static final long MAX_AWAIT_MILLIS = 15_000;

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

        ensureGroupKey(grpId, 1);

        checkEncryptedCaches(grids.get1(), grids.get2());
    }


    @Test
    public void testNodeFailsDuringPrepare() throws Exception {
        checkNodeFailsDuringRotation(false, true, 0);
    }

    @Test
    public void testCrdFailsDuringPrepare() throws Exception {
        checkNodeFailsDuringRotation(true, true, 0);
    }

    @Test
    public void testNodeFailsDuringPerform() throws Exception {
        checkNodeFailsDuringRotation(false, false, 1);
    }

    @Test
    public void testCrdFailsDuringPerform() throws Exception {
        checkNodeFailsDuringRotation(true, false, 1);
    }

    @Test
    public void testNotAllBltNodesPresent() throws Exception {
        checkNodeFailsDuringRotation(false, true, 0);
    }

    /**
     * @param stopCrd {@code True} if stop coordinator.
     * @param prepare {@code True} if stop on the prepare phase. {@code False} if stop on the perform phase.
     */
    private void checkNodeFailsDuringRotation(boolean stopCrd, boolean prepare, int expKeyId) throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        ensureGroupKey(grpId, 0);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grids.get2());

        AtomicBoolean preparePhase = new AtomicBoolean(true);

        spi.blockMessages((node, msg) -> {
            if (msg instanceof SingleNodeMessage) {
                boolean isPrepare = preparePhase.compareAndSet(true, false);

                return prepare || !isPrepare;
            }

            return false;
        });

        IgniteEx aliveNode = stopCrd ? grids.get2() : grids.get1();

        IgniteFuture<Void> fut = aliveNode.encryption().changeGroupKey(Collections.singleton(grpId));

        spi.waitForBlocked();

        runAsync(() -> {
            if (stopCrd)
                stopGrid(GRID_0, true);
            else
                stopGrid(GRID_1, true);
        });

        if (prepare)
            assertThrowsAnyCause(log, fut::get, IgniteCheckedException.class, null);
        else
            fut.get();

        assertEquals(expKeyId, aliveNode.context().encryption().groupKey(grpId).id());
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
        validateKeyIdentifier(node1.cachex(cacheName()).context(), 1);
        validateKeyIdentifier(node2.cachex(cacheName()).context(), 1);

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
        validateKeyIdentifier(node1.cachex(cacheName()).context(), 1);
        validateKeyIdentifier(node2.cachex(cacheName()).context(), 1);

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

    private void validateKeyIdentifier(GridCacheContext<Object, Object> ctx, int keyId) throws IgniteCheckedException, IOException {
        int grpId = ctx.groupId();

        int realPageSize = ctx.group().dataRegion().pageMemory().realPageSize(grpId);

        int encryptionBlockSize = ctx.shared().kernalContext().config().getEncryptionSpi().blockSize();

        List<Integer> parts = IntStream.range(0, ctx.shared().affinity().affinity(grpId).partitions())
            .boxed().collect(Collectors.toList());

        parts.add(INDEX_PARTITION);

        for (int p : parts) {
            FilePageStore pageStore =
                (FilePageStore)((FilePageStoreManager)ctx.shared().pageStore()).getStore(grpId, p);

            if (!pageStore.exists())
                continue;

            long metaPageId = PageIdUtils.pageId(p, PageIdAllocator.FLAG_DATA, 0);

            scanFileStore(pageStore, metaPageId, realPageSize, encryptionBlockSize, keyId);
        }
    }

    private void scanFileStore(FilePageStore pageStore, long startPageId, int realPageSize, int blockSize, int expKeyIdentifier) throws IgniteCheckedException, IOException {
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

    private void ensureGroupKey(int grpId, int keyId) throws IgniteCheckedException {
        for (Ignite g : G.allGrids()) {
            IgniteEx grid = (IgniteEx)g;

            if (grid.context().clientNode())
                continue;

            GridEncryptionManager encrMgr = grid.context().encryption();

            assertEquals(grid.localNode().id().toString(), keyId, encrMgr.groupKey(grpId).id());

            encrMgr.encryptionTask(grpId).get();
        }
    }
}
