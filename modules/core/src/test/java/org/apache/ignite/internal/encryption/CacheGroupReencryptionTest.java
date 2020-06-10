/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.encryption;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_DISABLED;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

public class CacheGroupReencryptionTest extends AbstractEncryptionTest {
    private static final long MAX_AWAIT_MILLIS = 15_000;

    private final FailingContext failCtx = new FailingContext();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024 * 1024 * 1024L)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalSegmentSize(10 * 1024 * 1024)
            .setWalSegments(4)
            .setMaxWalArchiveSize(10 * 1024 * 1024L)
            .setCheckpointFrequency(30 * 1000L)
            .setWalMode(LOG_ONLY)
            .setFileIOFactory(new FailingFileIOFactory(new RandomAccessFileIOFactory(), failCtx));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, String grp) {
        CacheConfiguration<K, V> cfg = super.cacheConfiguration(name, grp);

        return cfg.setAffinity(new RendezvousAffinityFunction(false, 16));
    }

    /**
     * Check physical recovery after checkpoint failure during re-encryption.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPhysicalRecovery() throws Exception {
        failCtx.cacheName = cacheName();
        failCtx.failFileName = INDEX_FILE_NAME;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2() ,cacheName(), null);

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        failCtx.failOffset = nodes.get1().context().cache().context().database().pageSize();

        nodes.get1().encryption().changeGroupKey(Collections.singleton(grpId)).get();

        awaitEncryption(G.allGrids(), grpId).get();

        assertThrowsAnyCause(log, () -> {
            forceCheckpoint();

            return null;
        }, IgniteCheckedException.class, null);

        stopAllGrids(true);

        failCtx.failFileName = null;

        nodes = startTestGrids(false);

        checkEncryptedCaches(nodes.get1(), nodes.get2());

        checkGroupKey(grpId, 1);
    }

    /**
     * Ensures that re-encryption continues after a restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLogicalRecovery() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null, true);

        loadData(100_000);

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeGroupKey(Collections.singleton(grpId)).get();

        awaitEncryption(G.allGrids(), grpId).get();

        assertEquals(1, node0.context().encryption().groupKey(grpId).id());
        assertEquals(1, node1.context().encryption().groupKey(grpId).id());

        stopAllGrids();

        info(">>> Start grids (iteration 1)");

        startTestGrids(false);

        enableCheckpoints(G.allGrids(), false);

        stopAllGrids();

        info(">>> Start grids (iteration 2)");

        startTestGrids(false);

        checkGroupKey(grpId, 1);
    }

    @Test
    @WithSystemProperty(key = IGNITE_REENCRYPTION_DISABLED, value = "true")
    public void testReencryptionRestart() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeGroupKey(Collections.singleton(grpId)).get();

        forceCheckpoint();

        stopAllGrids();

        nodes = startTestGrids(false);

        node0 = nodes.get1();
        node1 = nodes.get2();

        assertFalse(node0.context().encryption().encryptionStateTask(grpId).isDone());
        assertFalse(node1.context().encryption().encryptionStateTask(grpId).isDone());

        stopAllGrids();

        System.setProperty(IGNITE_REENCRYPTION_DISABLED, "false");

        startTestGrids(false);

        awaitEncryption(G.allGrids(), grpId).get(MAX_AWAIT_MILLIS);
    }

    @Test
    public void testKeyCleanup() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        int grpId = CU.cacheId(cacheName());

        Set<Long> walSegments = new HashSet<>();

        walSegments.add(node1.context().cache().context().wal().currentSegment());

        node0.encryption().changeGroupKey(Collections.singleton(grpId)).get();

        walSegments.add(node1.context().cache().context().wal().currentSegment());

        awaitEncryption(G.allGrids(), grpId).get();

        // Simulate that wal was removed.
        for (long segment : walSegments)
            node1.context().encryption().onWalSegmentRemoved(segment);

        stopGrid(GRID_1);

        node1 = startGrid(GRID_1);

        enableCheckpoints(G.allGrids(), true);

        node1.cluster().state(ClusterState.ACTIVE);

        node1.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

        checkEncryptedCaches(node0, node1);

        checkGroupKey(grpId, 1);
    }

    static final class FailingContext implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private volatile String failFileName;

        private volatile String cacheName;

        private volatile long failOffset = -1;
    }

    /** */
    static final class FailingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        private final FailingContext ctx;

        /**
         * @param factory Delegate factory.
         */
        FailingFileIOFactory(FileIOFactory factory, FailingContext ctx) {
            delegateFactory = factory;

            this.ctx = ctx;
        }

        /** {@inheritDoc}*/
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return file.getAbsolutePath().contains(CACHE_DIR_PREFIX + ctx.cacheName) &&
                file.getName().equals(ctx.failFileName) ? new FailingFileIO(delegate) : delegate;
        }

        /** */
        final class FailingFileIO extends FileIODecorator {
            /**
             * @param delegate File I/O delegate
             */
            public FailingFileIO(FileIO delegate) {
                super(delegate);
            }

            @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
                if (ctx.failOffset == position)
                    throw new IOException("Test exception.");

                return delegate.writeFully(srcBuf, position);
            }
        }
    }


}
