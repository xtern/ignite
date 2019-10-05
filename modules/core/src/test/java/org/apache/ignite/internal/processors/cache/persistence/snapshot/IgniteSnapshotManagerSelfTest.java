/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DELTA_SUFFIX;

/**
 * TODO backup must fail in case of parallel cache stop operation
 */
public class IgniteSnapshotManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final FileIOFactory DFLT_IO_FACTORY = new RandomAccessFileIOFactory();

    /** */
    private static final String SNAPSHOT_NAME = "testSnapshot";

    /** */
    private static final int CACHE_PARTS_COUNT = 8;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final int CACHE_KEYS_RANGE = 1024;

    /** */
    private static final PathMatcher DELTA_FILE_MATCHER =
        FileSystems.getDefault().getPathMatcher("glob:**" + DELTA_SUFFIX);

    /** */
    private static final DataStorageConfiguration memCfg = new DataStorageConfiguration()
        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100L * 1024 * 1024)
            .setPersistenceEnabled(true))
        .setPageSize(PAGE_SIZE)
        .setWalMode(WALMode.LOG_ONLY);

    /** */
    private CacheConfiguration<Integer, Integer> defaultCacheCfg =
        new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

    /**
     * Calculate CRC for all partition files of specified cache.
     *
     * @param cacheDir Cache directory to iterate over partition files.
     * @return The map of [fileName, checksum].
     * @throws IgniteCheckedException If fails.
     */
    private static Map<String, Integer> calculateCRC32Partitions(File cacheDir) throws IgniteCheckedException {
        assert cacheDir.isDirectory() : cacheDir.getAbsolutePath();

        Map<String, Integer> result = new HashMap<>();

        try {
            try (DirectoryStream<Path> partFiles = newDirectoryStream(cacheDir.toPath(),
                p -> p.toFile().getName().startsWith(PART_FILE_PREFIX) && p.toFile().getName().endsWith(FILE_SUFFIX))
            ) {
                for (Path path : partFiles)
                    result.put(path.toFile().getName(), FastCrc.calcCrc(path.toFile()));
            }

            return result;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** */
    @Before
    public void beforeTestSnapshot() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void afterTestSnapshot() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(memCfg)
            .setCacheConfiguration(defaultCacheCfg);
    }

    /**
     *
     */
    @Test
    public void testSnapshotLocalPartitions() throws Exception {
        // Start grid node with data before each test.
        IgniteEx ig = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);

        for (int i = CACHE_KEYS_RANGE; i < 2048; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        try (IgniteDataStreamer<Integer, TestOrderItem> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 2048; i++)
                ds.addData(i, new TestOrderItem(i, i));
        }

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        IgniteInternalFuture<?> snpFut = mgr.createLocalSnapshot(SNAPSHOT_NAME,
            Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME)));

        snpFut.get();

        File cacheWorkDir = ((FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore())
            .cacheWorkDir(defaultCacheCfg);

        // Calculate CRCs
        final Map<String, Integer> origParts = calculateCRC32Partitions(cacheWorkDir);

        final Map<String, Integer> bakcupCRCs = calculateCRC32Partitions(new File(mgr.snapshotDir(SNAPSHOT_NAME),
            cacheDirName(defaultCacheCfg)));

        assertEquals("Partiton must have the same CRC after shapshot and after merge", origParts, bakcupCRCs);

        try (DirectoryStream<Path> files = Files.newDirectoryStream(
            cacheWorkDir(new File(mgr.snapshotWorkDir(), SNAPSHOT_NAME), cacheDirName(defaultCacheCfg)).toPath(),
            DELTA_FILE_MATCHER::matches)) {
            assertFalse(".delta files must be cleaned after snapshot", files.iterator().hasNext());
        }
    }

    /**
     *
     */
    @Test
    public void testSnapshotLocalPartitionsNextCpStarted() throws Exception {
        final int value_multiplier = 2;
        CountDownLatch slowCopy = new CountDownLatch(1);

        IgniteEx ig = startGridWithCache(defaultCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()
            .setPartitions(CACHE_PARTS_COUNT)), CACHE_KEYS_RANGE);

        Map<Integer, Set<Integer>> parts = new HashMap<>();

        parts.put(CU.cacheId(DEFAULT_CACHE_NAME),
            Stream.iterate(0, n -> n + 1)
                .limit(CACHE_PARTS_COUNT) // With index partition
                .collect(Collectors.toSet()));

        parts.computeIfAbsent(CU.cacheId(DEFAULT_CACHE_NAME), p -> new HashSet<>())
            .add(PageIdAllocator.INDEX_PARTITION);

        FilePageStoreManager storeMgr = (FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore();


        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        File cacheWorkDir = storeMgr.cacheWorkDir(defaultCacheCfg);
        File cpDir = ((GridCacheDatabaseSharedManager) ig.context().cache().context().database())
            .checkpointDirectory();
        File walDir = ((FileWriteAheadLogManager) ig.context().cache().context().wal()).walWorkDir();
        File cacheBackup = cacheWorkDir(mgr.snapshotDir(SNAPSHOT_NAME), defaultCacheCfg);

        // Change data before backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, value_multiplier * i);

        File snapshotDir0 = mgr.snapshotDir(SNAPSHOT_NAME);

        IgniteInternalFuture<?> snpFut = mgr
            .scheduleSnapshot(SNAPSHOT_NAME,
                parts,
                snapshotDir0,
                mgr.snapshotExecutorService(),
                new DeleagateSnapshotReceiver(mgr.localSnapshotReceiver(snapshotDir0)) {
                    @Override
                    public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                        try {
                            if (pair.getPartitionId() == 0)
                                U.await(slowCopy);

                            super.receivePart(part, cacheDirName, pair, length);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                });

        // Change data after backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 3 * i);

        // Backup on the next checkpoint must copy page before write it to partition
        CheckpointFuture cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("second cp");

        cpFut.finishFuture().get();

        slowCopy.countDown();

        snpFut.get();

        // Now can stop the node and check created backups.

        stopGrid(0);

        delete(cpDir.toPath());
        delete(walDir.toPath());

        Files.walk(cacheBackup.toPath())
            .map(Path::toFile)
            .forEach(System.out::println);

        // copy all backups to the cache directory
        Files.walk(cacheBackup.toPath())
            .map(Path::toFile)
            .filter(f -> !f.isDirectory())
            .forEach(f -> {
                try {
                    File target = new File(cacheWorkDir, f.getName());

                    Files.copy(f.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            });


        IgniteEx ig2 = startGrid(0);

        ig2.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(i * value_multiplier, ig2.cache(DEFAULT_CACHE_NAME).get(i));
    }

    /**
     *
     */
    @Test(expected = IgniteCheckedException.class)
    public void testSnapshotLocalPartitionNotEnoughSpace() throws Exception {
        final AtomicInteger throwCntr = new AtomicInteger();

        IgniteEx ig = startGridWithCache(defaultCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()
            .setPartitions(CACHE_PARTS_COUNT)), CACHE_KEYS_RANGE);

        // Change data after backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 2 * i);

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        mgr.ioFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = DFLT_IO_FACTORY.create(file, modes);

                if (file.getName().equals(IgniteSnapshotManager.getPartitionDeltaFileName(0)))
                    return new FileIODecorator(fileIo) {
                        @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                            if (throwCntr.incrementAndGet() == 3)
                                throw new IOException("Test exception. Not enough space.");

                            return super.writeFully(srcBuf);
                        }
                    };

                return fileIo;
            }
        });

        IgniteInternalFuture<?> backupFut = mgr.createLocalSnapshot(SNAPSHOT_NAME,
            Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME)));

        backupFut.get();
    }

    /**
     *
     */
    @Test(expected = IgniteCheckedException.class)
    public void testSnapshotCreateLocalCopyPartitionFail() throws Exception {
        IgniteEx ig = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);

        Map<Integer, Set<Integer>> parts = new HashMap<>();

        parts.computeIfAbsent(CU.cacheId(DEFAULT_CACHE_NAME), c -> new HashSet<>())
            .add(0);

        FilePageStoreManager storeMgr = (FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore();

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        File snpDir0 = new File(mgr.snapshotWorkDir(), SNAPSHOT_NAME);

        IgniteInternalFuture<?> fut = mgr.scheduleSnapshot(SNAPSHOT_NAME,
            parts,
            snpDir0,
            mgr.snapshotExecutorService(),
            new DeleagateSnapshotReceiver(mgr.localSnapshotReceiver(snpDir0)) {
                @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    if (pair.getPartitionId() == 0)
                        throw new IgniteException("Test. Fail to copy partition: " + pair);

                    super.receivePart(part, cacheDirName, pair, length);
                }
            });

        fut.get();
    }

    /**
     * @param dir Directory to delete.
     * @throws IOException If fails.
     */
    private static void delete(Path dir) throws IOException {
        Files.walk(dir)
            .map(Path::toFile)
            .forEach(File::delete);

        Files.delete(dir);

        assertFalse("Directory still exists",
            Files.exists(dir));
    }

    /**
     * @param ccfg Default cache configuration.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    private IgniteEx startGridWithCache(CacheConfiguration<Integer, Integer> ccfg, int range) throws Exception {
        defaultCacheCfg = ccfg;

        // Start grid node with data before each test.
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        for (int i = 0; i < range; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointFuture cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.finishFuture().get();

        return ig;
    }

    /**
     *
     */
    private static class ZeroPartitionAffinityFunction extends RendezvousAffinityFunction {
        @Override public int partition(Object key) {
            return 0;
        }
    }

    /**
     *
     */
    private static class DeleagateSnapshotReceiver implements SnapshotReceiver {
        /** Delegate call to. */
        private final SnapshotReceiver delegate;

        /**
         * @param delegate Delegate call to.
         */
        public DeleagateSnapshotReceiver(SnapshotReceiver delegate) {
            this.delegate = delegate;
        }

        @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            delegate.receivePart(part, cacheDirName, pair, length);
        }

        @Override public void receiveDelta(File delta, String cacheDirName, GroupPartitionId pair) {
            delegate.receiveDelta(delta, cacheDirName, pair);
        }

        @Override public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     *
     */
    private static class TestOrderItem implements Serializable {
        /** Order key. */
        private final int key;

        /** Order value. */
        private final int value;

        public TestOrderItem(int key, int value) {
            this.key = key;
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestOrderItem item = (TestOrderItem)o;

            return key == item.key &&
                value == item.value;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(key, value);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestOrderItem [key=" + key + ", value=" + value + ']';
        }
    }
}
