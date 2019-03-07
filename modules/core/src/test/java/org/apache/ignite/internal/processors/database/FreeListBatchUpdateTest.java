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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
//import ru.sbrf.gg.load.LoadTable;
//import ru.sbrf.gg.load.ProcessTableFile;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.junit.Assert.assertArrayEquals;

/**
 *
 */
//@RunWith(JUnit4.class)
@RunWith(Parameterized.class)
public class FreeListBatchUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int HDR_SIZE = 8 + 32;

    /** */
    private static final long DEF_REG_SIZE = 6 * 1024 * 1024 * 1024L;

    /** */
    private static final String DEF_CACHE_NAME = "DepoHist_DPL_union-module";

    /** */
    @Parameterized.Parameters(name = "with atomicity={0} and persistence={1}")
    public static Iterable<Object[]> setup() {
        return Arrays.asList(new Object[][]{
//            {CacheAtomicityMode.ATOMIC, false},
//            {CacheAtomicityMode.ATOMIC, true},
            {CacheAtomicityMode.TRANSACTIONAL, false},
//            {CacheAtomicityMode.TRANSACTIONAL, true},
//            {CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, false},
//            {CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, true}
        });
    }

    @Parameterized.Parameter(0)
    public CacheAtomicityMode cacheAtomicityMode;

    @Parameterized.Parameter(1)
    public boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration def = new DataRegionConfiguration();
        def.setInitialSize(3400 * 1024 * 1024L);
        def.setMaxSize(DEF_REG_SIZE);
        def.setPersistenceEnabled(persistence);

        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        storeCfg.setDefaultDataRegionConfiguration(def);

        if (persistence) {
            storeCfg.setWalMode(WALMode.LOG_ONLY);
            storeCfg.setMaxWalArchiveSize(Integer.MAX_VALUE);
        }

        cfg.setDataStorageConfiguration(storeCfg);

        return cfg;
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);
    }

    @Test
    public void checkStreamer() throws Exception {
        Ignite node = startGrids(4);

        node.cluster().active(true);

        IgniteCache<String, byte[]> cache = node.createCache(ccfg(8, CacheMode.REPLICATED));

        awaitPartitionMapExchange();

        int cnt = 1024;

        //IgniteCache<String, byte[]> cache = ;

        try (IgniteDataStreamer<String, byte[]> streamer = node.dataStreamer(DEF_CACHE_NAME)) {

            for (int i = 0; i < cnt; i++)
                streamer.addData(String.valueOf(i), new byte[128]);
        }

        log.info("Sleep");

        U.sleep(5_000);

        assert GridTestUtils.waitForCondition(() -> {
            return cache.size() == cnt;
        }, 10_000);

        for (int i = 0; i < cnt; i++)
            assertTrue(cache.get(String.valueOf(i)).length == 128);
    }

    /**
     *
     */
    @Test
    public void testBatchPartialRebalance() throws Exception {
        if (!persistence)
            return;

        // TODO https://issues.apache.org/jira/browse/IGNITE-7384
        // http://apache-ignite-developers.2346864.n4.nabble.com/Historical-rebalance-td38380.html
        if (cacheAtomicityMode == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            return;

        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "100");

        Ignite node = startGrids(2);

        node.cluster().active(true);

        IgniteCache<String, byte[]> cache = node.createCache(ccfg());

        int cnt = 10_000;

        log.info("Loading " + cnt + " random entries.");

        Map<String, byte[]> srcMap = new HashMap<>();

        for (int i = 0; i < cnt; i++) {
            byte[] obj = new byte[ThreadLocalRandom.current().nextInt(1024)];

            srcMap.put(String.valueOf(i), obj);
        }

        try (IgniteDataStreamer<String, byte[]> streamer = node.dataStreamer(DEF_CACHE_NAME)) {
            streamer.addData(srcMap);
        }

        forceCheckpoint();

        log.info("Stopping node #2.");

        grid(1).close();

        log.info("Updating values on node #1.");

        for (int i = 100; i < 1000; i++) {
            String key = String.valueOf(i);

            if (i % 33 == 0) {
                cache.remove(key);

                srcMap.remove(key);
            }
            else {
                byte[] bytes = cache.get(key);

                Arrays.fill(bytes, (byte)1);

                srcMap.put(key, bytes);
                cache.put(key, bytes);
            }
        }

        forceCheckpoint();

        log.info("Starting node #2.");

        IgniteEx node2 = startGrid(1);

        log.info("Await rebalance on node #2.");

        awaitRebalance(node2, DEF_CACHE_NAME);

        log.info("Stop node #1.");

        node.close();

        validateCacheEntries(node2.cache(DEF_CACHE_NAME), srcMap);
    }


    /**
     *
     */
    @Test
    public void testBatchPutAll() throws Exception {
        Ignite node = startGrid(0);

        node.cluster().active(true);

        node.createCache(ccfg());

        int cnt = 1_000_000;
        int minSize = 0;
        int maxSize = 2048;
        int start = 0;

        log.info("Loading " + cnt + " random entries per " + minSize + " - " + maxSize + " bytes.");

        Map<String, byte[]> srcMap = new HashMap<>();

        for (int i = start; i < start + cnt; i++) {
            int size = minSize + ThreadLocalRandom.current().nextInt(maxSize - minSize);

            byte[] obj = new byte[size];

            srcMap.put(String.valueOf(i), obj);
        }

        try (IgniteDataStreamer<String, byte[]> streamer = node.dataStreamer(DEF_CACHE_NAME)) {
            streamer.addData(srcMap);
        }

        srcMap.put(String.valueOf(1), new byte[65536]);

        node.cache(DEF_CACHE_NAME).put(String.valueOf(1), new byte[65536]);

        log.info("Done");

        IgniteCache cache = node.cache(DEF_CACHE_NAME);

        if (persistence)
            node.cluster().active(false);

        final IgniteEx node2 = startGrid(1);

        if (persistence) {
            List<BaselineNode> list = new ArrayList<>(node.cluster().currentBaselineTopology());

            list.add(node2.localNode());

            node.cluster().active(true);

            node.cluster().setBaselineTopology(list);
        }

        log.info("await rebalance");

        awaitRebalance(node2, DEF_CACHE_NAME);

        U.sleep(2_000);

        node.close();

        log.info("Verification on node2");

        validateCacheEntries(node2.cache(DEF_CACHE_NAME), srcMap);

        if (persistence) {
            node2.close();

            Ignite ignite = startGrid(1);

            ignite.cluster().active(true);

            log.info("Validate entries after restart");

            validateCacheEntries(ignite.cache(DEF_CACHE_NAME), srcMap);
        }
    }

    /**
     *
     */
//    @Test
//    public void testBatchPutAllLoader() throws Exception {
//        Ignite node = startGrid(0);
//
//        node.cluster().active(true);
//
//        node.createCache(ccfg());
//
//        ExecutorService execSvc = Executors.newFixedThreadPool(4);
//
//        ProcessTableFile load = new LoadTable("EIP_DBAOSB_DEPOHISTPARAM", "/home/xtern/src/data/cod_data_mini.zip", execSvc, node, 1);
//
//        load.process();
//
//        execSvc.shutdown();
//
//        log.info("Done");
//
//        IgniteCache cache = node.cache(DEF_CACHE_NAME);
//
//        if (persistence)
//            node.cluster().active(false);
//
//        final IgniteEx node2 = startGrid(1);
//
//        if (persistence) {
//            List<BaselineNode> list = new ArrayList<>(node.cluster().currentBaselineTopology());
//
//            list.add(node2.localNode());
//
//            node.cluster().active(true);
//
//            node.cluster().setBaselineTopology(list);
//        }
//
//        log.info("await rebalance");
//
//        awaitRebalance(node2, DEF_CACHE_NAME);
//
//        U.sleep(2_000);
//
//        node.close();
//
//        log.info("Verification on node2");
//    }

    /**
     * @param node Ignite node.
     * @param name Cache name.
     */
    private void awaitRebalance(IgniteEx node, String name) throws IgniteInterruptedCheckedException {
        boolean ok = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for ( GridDhtLocalPartition part : node.context().cache().cache(name).context().group().topology().localPartitions()) {
                    if (part.state() != GridDhtPartitionState.OWNING)
                        return false;
                }

                return true;
            }
        }, 60_000);

        U.sleep(1000);

        assertTrue(ok);
    }

    /**
     * @param cache Cache.
     * @param map Map.
     */
    @SuppressWarnings("unchecked")
    private void validateCacheEntries(IgniteCache cache, Map<String, byte[]> map) {
        if (true)
            return;

        log.info("Cache validation: " + map.size());

        assertEquals(map.size(), cache.size());

        for (Map.Entry<String, byte[]> e : map.entrySet()) {
            String idx = "idx=" + e.getKey();

            byte[] bytes = (byte[])cache.get(e.getKey());

            assertNotNull(idx, bytes);

            assertEquals(idx + ": length not equal", e.getValue().length, bytes.length);

            assertArrayEquals(idx, e.getValue(), bytes);
        }
    }


    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg() {
        return ccfg(1024, CacheMode.REPLICATED);
    }

    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg(int parts, CacheMode mode) {
        return new CacheConfiguration<K, V>(DEF_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setCacheMode(mode)
            .setAtomicityMode(cacheAtomicityMode);
    }

    @Test
    public void testFUCK() {
        ///home/xtern/tools/sdk/jdk1.8.0_152/bin/java -ea -Xmx2G -classpath
        //com.intellij.rt.execution.junit.JUnitStarter -ideVersion5 -junit4 org.apache.ignite.internal.processors.database.FreeListBatchBench,testBatch
        String cp = "/home/xtern/tools/ide/idea-IU-182.4505.22/lib/idea_rt.jar:/home/xtern/tools/ide/idea-IU-182.4505.22/plugins/junit/lib/junit-rt.jar:/home/xtern/tools/ide/idea-IU-182.4505.22/plugins/junit/lib/junit5-rt.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/charsets.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/deploy.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/cldrdata.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/dnsns.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/jaccess.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/jfxrt.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/localedata.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/nashorn.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/sunec.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/sunjce_provider.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/sunpkcs11.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/ext/zipfs.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/javaws.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/jce.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/jfr.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/jfxswt.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/jsse.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/management-agent.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/plugin.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/resources.jar:/home/xtern/tools/sdk/jdk1.8.0_152/jre/lib/rt.jar:/home/xtern/src/ignite/modules/core/target/test-classes:/home/xtern/src/ignite/modules/core/target/classes:/home/xtern/.m2/repository/javax/cache/cache-api/1.0.0/cache-api-1.0.0.jar:/home/xtern/.m2/repository/org/jetbrains/annotations/16.0.3/annotations-16.0.3.jar:/home/xtern/.m2/repository/mx4j/mx4j-tools/3.0.1/mx4j-tools-3.0.1.jar:/home/xtern/.m2/repository/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar:/home/xtern/.m2/repository/commons-dbcp/commons-dbcp/1.4/commons-dbcp-1.4.jar:/home/xtern/.m2/repository/commons-pool/commons-pool/1.5.4/commons-pool-1.5.4.jar:/home/xtern/.m2/repository/com/thoughtworks/xstream/xstream/1.4.8/xstream-1.4.8.jar:/home/xtern/.m2/repository/xmlpull/xmlpull/1.1.3.1/xmlpull-1.1.3.1.jar:/home/xtern/.m2/repository/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar:/home/xtern/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:/home/xtern/.m2/repository/org/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar:/home/xtern/.m2/repository/com/h2database/h2/1.4.197/h2-1.4.197.jar:/home/xtern/.m2/repository/org/mockito/mockito-all/1.9.5/mockito-all-1.9.5.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-servlets/9.4.11.v20180605/jetty-servlets-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-continuation/9.4.11.v20180605/jetty-continuation-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-http/9.4.11.v20180605/jetty-http-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-util/9.4.11.v20180605/jetty-util-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-io/9.4.11.v20180605/jetty-io-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-webapp/9.4.11.v20180605/jetty-webapp-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-xml/9.4.11.v20180605/jetty-xml-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-servlet/9.4.11.v20180605/jetty-servlet-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-security/9.4.11.v20180605/jetty-security-9.4.11.v20180605.jar:/home/xtern/.m2/repository/org/eclipse/jetty/jetty-server/9.4.11.v20180605/jetty-server-9.4.11.v20180605.jar:/home/xtern/.m2/repository/javax/servlet/javax.servlet-api/3.1.0/javax.servlet-api-3.1.0.jar:/home/xtern/.m2/repository/com/esotericsoftware/kryo/kryo/2.20/kryo-2.20.jar:/home/xtern/.m2/repository/com/esotericsoftware/reflectasm/reflectasm/1.07/reflectasm-1.07-shaded.jar:/home/xtern/.m2/repository/org/ow2/asm/asm/4.0/asm-4.0.jar:/home/xtern/.m2/repository/com/esotericsoftware/minlog/minlog/1.2/minlog-1.2.jar:/home/xtern/.m2/repository/org/objenesis/objenesis/1.2/objenesis-1.2.jar:/home/xtern/.m2/repository/c3p0/c3p0/0.9.1/c3p0-0.9.1.jar:/home/xtern/.m2/repository/org/gridgain/ignite-shmem/1.0.0/ignite-shmem-1.0.0.jar:/home/xtern/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar:/home/xtern/.m2/repository/org/springframework/spring-beans/4.3.18.RELEASE/spring-beans-4.3.18.RELEASE.jar:/home/xtern/.m2/repository/org/springframework/spring-core/4.3.18.RELEASE/spring-core-4.3.18.RELEASE.jar:/home/xtern/.m2/repository/commons-logging/commons-logging/1.2/commons-logging-1.2.jar:/home/xtern/.m2/repository/org/springframework/spring-context/4.3.18.RELEASE/spring-context-4.3.18.RELEASE.jar:/home/xtern/.m2/repository/org/springframework/spring-aop/4.3.18.RELEASE/spring-aop-4.3.18.RELEASE.jar:/home/xtern/.m2/repository/org/springframework/spring-expression/4.3.18.RELEASE/spring-expression-4.3.18.RELEASE.jar:/home/xtern/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar:/home/xtern/.m2/repository/org/apache/ignite/binary/test1/1.1/test1-1.1.jar:/home/xtern/.m2/repository/org/apache/ignite/binary/test2/1.1/test2-1.1.jar:/home/xtern/.m2/repository/com/google/guava/guava/25.1-jre/guava-25.1-jre.jar:/home/xtern/.m2/repository/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar:/home/xtern/.m2/repository/org/checkerframework/checker-qual/2.0.0/checker-qual-2.0.0.jar:/home/xtern/.m2/repository/com/google/errorprone/error_prone_annotations/2.1.3/error_prone_annotations-2.1.3.jar:/home/xtern/.m2/repository/com/google/j2objc/j2objc-annotations/1.1/j2objc-annotations-1.1.jar:/home/xtern/.m2/repository/org/codehaus/mojo/animal-sniffer-annotations/1.14/animal-sniffer-annotations-1.14.jar:/home/xtern/.m2/repository/org/javassist/javassist/3.20.0-GA/javassist-3.20.0-GA.jar:/home/xtern/.m2/repository/junit/junit/4.11/junit-4.11.jar:/home/xtern/.m2/repository/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar:/home/xtern/.m2/repository/ignite-cod-data-loader/ignite-cod-data-loader/0.0.1-SNAPSHOT/ignite-cod-data-loader-0.0.1-SNAPSHOT.jar:/home/xtern/.m2/repository/org/scala-lang/scala-library/2.11.8/scala-library-2.11.8.jar:/home/xtern/.m2/repository/org/scala-lang/modules/scala-xml_2.11/1.0.6/scala-xml_2.11-1.0.6.jar:/home/xtern/.m2/repository/com/github/scopt/scopt_2.11/3.7.0/scopt_2.11-3.7.0.jar:/home/xtern/src/ignite/modules/spring/target/classes:/home/xtern/.m2/repository/org/springframework/spring-tx/4.3.18.RELEASE/spring-tx-4.3.18.RELEASE.jar:/home/xtern/.m2/repository/org/springframework/spring-jdbc/4.3.18.RELEASE/spring-jdbc-4.3.18.RELEASE.jar:/home/xtern/src/ignite/modules/log4j/target/classes:/home/xtern/src/ignite/modules/indexing/target/classes:/home/xtern/.m2/repository/commons-codec/commons-codec/1.11/commons-codec-1.11.jar:/home/xtern/.m2/repository/org/apache/lucene/lucene-core/7.4.0/lucene-core-7.4.0.jar:/home/xtern/.m2/repository/org/apache/lucene/lucene-analyzers-common/7.4.0/lucene-analyzers-common-7.4.0.jar:/home/xtern/.m2/repository/org/apache/lucene/lucene-queryparser/7.4.0/lucene-queryparser-7.4.0.jar:/home/xtern/.m2/repository/org/apache/lucene/lucene-queries/7.4.0/lucene-queries-7.4.0.jar:/home/xtern/.m2/repository/org/apache/lucene/lucene-sandbox/7.4.0/lucene-sandbox-7.4.0.jar:/home/xtern/src/ignite/modules/spring-data/target/classes:/home/xtern/.m2/repository/org/springframework/data/spring-data-commons/1.13.14.RELEASE/spring-data-commons-1.13.14.RELEASE.jar:/home/xtern/.m2/repository/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:/home/xtern/.m2/repository/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar:/home/xtern/.m2/repository/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar:/home/xtern/.m2/repository/commons-lang/commons-lang/2.6/commons-lang-2.6.jar:/home/xtern/tools/sdk/jdk1.8.0_152/lib/tools.jar";
        Arrays.stream(cp.split(":")).forEach(v -> cp(v, "/home/xtern/out"));


    }

    private void cp(String path, String target) {

        File source = new File(path);
        File target0 = new File(target + "/" + source.getName());

        System.out.println("> cp " + source + " -> " + target0);

        try {
            if (source.isDirectory())
                FileUtils.copyDirectory(source, target0);
            else
                FileUtils.copyFile(source, target0);
        } catch (IOException e) {
            throw  new RuntimeException(e.getMessage());
        }


        //Files.

    }

//    private static final String delim =  ";";
//    private static final String encoding = "ISO-8859-1";
////    class CSVReader {
////
////        final int maxLines;
////
////        final InputStream file;
////
////        int cnt = 0;
////
////        CSVReader(InputStream file) {
////            maxLines = Integer.valueOf(System.getProperty("MAX_LINES", "-1"));
////
////            if (maxLines != -1)
////                log.warning(maxLines + " would be readed from stream");
////        }
////    }
////
////
////
////
////        override def iterator: Iterator[Array[String]] = new Iterator[Array[String]] {
////            override def hasNext: Boolean = {
////                cnt += 1
////                (maxLines == -1 || cnt < maxLines) && lines.hasNext
////            }
////
////            override def next(): Array[String] = {
////                val line = lines.next()
////                line.split(delim, -1)
////            }
////        }
////    }
}
