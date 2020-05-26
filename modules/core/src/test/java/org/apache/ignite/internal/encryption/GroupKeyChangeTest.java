package org.apache.ignite.internal.encryption;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/**
 *
 */
public class GroupKeyChangeTest extends AbstractEncryptionTest {
    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    private static final long MAX_AWAIT_MILLIS = 15_000;

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
    public void testBasicChangeUnderLoad() throws Exception {
        startTestGrids(true);

        final IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        IgniteInternalCache<Object, Object> cache = node1.cachex(cacheName());

        AtomicInteger cntr = new AtomicInteger(cache.size());

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            try (IgniteDataStreamer<Integer, String> streamer = node1.dataStreamer(cacheName())) {
                while (!Thread.currentThread().isInterrupted()) {
                    int n = cntr.getAndIncrement();

                    streamer.addData(n, String.valueOf(n));
                }
            }
        });

        U.sleep(500);



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

        IgniteEx node = startGrid(GRID_0);
        node2 = startGrid(GRID_1);

        node.cluster().state(ClusterState.ACTIVE);

        GridEncryptionManager encMgr1 = node.context().encryption();
        GridEncryptionManager encMgr2 = node2.context().encryption();

        try (IgniteDataStreamer<Integer, String> streamer = node.dataStreamer(cacheName())) {
            for (; ; ) {
                int n = cntr.getAndIncrement();

                streamer.addData(n, String.valueOf(n));

                if (n % 1000 == 0 && encMgr1.groupKeysInfo(grpId).size() == 1 && encMgr2.groupKeysInfo(grpId).size() == 1)
                    break;

                if (n > 10_000_000)
                    fail("Keys not cleared");
            }
        }

        assertEquals(1, node.context().encryption().groupKeysInfo(grpId).size());
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

//    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
//
//    public static String bytesToHex(byte[] bytes) {
//        char[] hexChars = new char[bytes.length * 2];
//        for (int j = 0; j < bytes.length; j++) {
//            int v = bytes[j] & 0xFF;
//            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
//            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
//        }
//
//        StringBuilder buf = new StringBuilder();
//
//        for (int j = 0; j < bytes.length; j++) {
//            if (j != 0 && j % 4 == 0)
//                buf.append(" | ");
//
//            if (j != 0 && j % 16 == 0)
//                buf.append('\n');
//
//            buf.append(hexChars[j * 2]);
//            buf.append(hexChars[j * 2 + 1]);
//            buf.append(' ');
//        }
//
//        return buf.toString();
//    }
}
