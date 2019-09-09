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

package org.apache.ignite.internal.processors.database;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.preload.GridCachePreloadSharedManager;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.io.GridFileUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.IgniteUtils.GB;

/**
 *
 */
public class GridCacheStoreReinitializationTest extends GridCommonAbstractTest {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
//        ccfg.setBackups(2);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 4));
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);
        //ccfg.setBackups(1);
        // todo check different sync modes
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dscfg = new DataStorageConfiguration();

        dscfg.setWalMode(WALMode.LOG_ONLY);

        DataRegionConfiguration reg = new DataRegionConfiguration();

        reg.setMaxSize(2 * GB);
        reg.setPersistenceEnabled(true);

        dscfg.setDefaultDataRegionConfiguration(reg);
        dscfg.setCheckpointFrequency(3_000);
        dscfg.setMaxWalArchiveSize(10 * GB);

        cfg.setDataStorageConfiguration(dscfg);

        return cfg;
    }

    /** */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();
    }

    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void checkInitPartition() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);
        node0.cluster().baselineAutoAdjustTimeout(0);

        IgniteEx node1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteInternalCache<Integer, Integer> cache = node0.cachex(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint(node0);
        forceCheckpoint(node1);

        stopGrid(1);

        U.sleep(5_000);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        forceCheckpoint(node0);

        List<GridDhtLocalPartition> parts = cache.context().topology().localPartitions();

        File[] partFiles = new File[parts.size()];

        for (GridDhtLocalPartition part : parts) {
            File file = partitionFile(node0, DEFAULT_CACHE_NAME, part.id());

            System.out.println(">> (exists=" + (file.exists()) +") " + file);

            partFiles[part.id()] = file;
        }

        stopGrid(0);

        // copy files to temp dir
        String sep = File.separator;

        File tmpDir = new File(System.getProperty("java.io.tmpdir") + sep + getClass().getName());

        if (!tmpDir.exists())
            tmpDir.mkdirs();

        try {
            assert tmpDir.isDirectory();

            assert tmpDir.listFiles().length == 0 : "Temp location is not empty: " + tmpDir;

            copyFiles(tmpDir.toString(), partFiles);

            node0 = startGrid(0);

            node0.cluster().active(true);
            node0.cluster().baselineAutoAdjustTimeout(0);

            cache = node0.cachex(DEFAULT_CACHE_NAME);

            CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

            assertEquals(20_000, cache.localSize(peekAll));

            System.out.println(">>> ");
            System.out.println(">>> starting node 1 ");
            System.out.println(">>> ");

            node1 = startGrid(1);

            awaitPartitionMapExchange();

            IgniteInternalCache<Object, Object> cache1 = node1.cachex(DEFAULT_CACHE_NAME);

            for (GridDhtLocalPartition p : cache1.context().topology().localPartitions())
                System.out.println(">xxx> " + p.id() + " " + p.state() + " size = " + p.fullSize());

            GridCacheContext<Object, Object> cctx = cache1.context();

            GridCachePreloadSharedManager preloader = node1.context().cache().context().preloader();

            GridCompoundFuture<Void,Void> fut = new GridCompoundFuture<>();

            fut.markInitialized();

            // Destroy partitions.
            for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
                part.moving();

                node1.context().cache().context().database().checkpointReadLock();

                try {
                    // Switching mode under the write lock.
                    part.readOnly(true);
                } finally {
                    node1.context().cache().context().database().checkpointReadUnlock();
                }

                // Simulating that part was downloaded and compltely destroying partition.
                fut.add(preloader.destroyPartition(part));
            }

            fut.get();

            forceCheckpoint(node1);

            // Init partitions.
            for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
                node1.context().cache().context().database().checkpointReadLock();

                try {
                    preloader.recoverPartition(part.id(), partFiles[part.id()], cctx);
                } finally {
                    node1.context().cache().context().database().checkpointReadUnlock();
                }

                assertTrue(cctx.topology().own(part));

                assertEquals(OWNING, cctx.topology().partitionState(node1.localNode().id(), part.id()));
            }

            int total = 44_000;

            for (int i = 20_000; i < total; i++)
                cache.put(i, i);

            for (GridDhtLocalPartition part : cctx.topology().localPartitions())
                assertEquals(total / cctx.topology().localPartitions().size(), part.fullSize());
        } finally {
            for (File f : tmpDir.listFiles())
                f.delete();
        }
    }

    private void copyFiles(String dir, File[] files) throws IOException {
        log.info(">> copy files:");

        for (int i = 0; i < files.length; i++) {
            File src = files[i];
            File dst = new File(dir + File.separator + src.getName());

            log.info("\n\t\t" + src + " (size=" + src.length() + ")\n\t\t   \\---> " + dst);

            RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

            GridFileUtils.copy(ioFactory, src, ioFactory, dst, Long.MAX_VALUE);

            files[i] = dst;
        }
    }

    private void validateLocal(IgniteInternalCache<Integer, Integer> cache, int size) throws IgniteCheckedException {
        assertEquals(size, cache.size());

        for (int i = 0; i < size; i++)
            assertEquals(String.valueOf(i), Integer.valueOf(i), cache.get(i));
    }

    private void overwritePartitionFile(Ignite node, int partId, File src) throws IgniteCheckedException, IOException {

    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param partId Partition id.
     */
    private File partitionFile(Ignite ignite, String cacheName, int partId) throws IgniteCheckedException {
        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeName = ignite.name().replaceAll("\\.", "_");

        return new File(dbDir, String.format("%s/cache-%s/part-%d.bin", nodeName, cacheName, partId));
//        String nodeName = ignite.cluster().localNode().attribute(ATTR_IGNITE_INSTANCE_NAME);
//
//        int idx = getTestIgniteInstanceIndex(nodeName);
//
//        //String nodeName = "node0" + backupIndex + "-" + ignite.cluster().localNode().consistentId();
//
//        return new File(dbDir, String.format("node%02d-%s/cache-%s/part-%d.bin",
//            idx, ignite.cluster().localNode().consistentId(), cacheName, partId));
    }
}
