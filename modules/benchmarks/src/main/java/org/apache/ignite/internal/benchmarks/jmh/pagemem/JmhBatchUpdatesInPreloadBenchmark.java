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

package org.apache.ignite.internal.benchmarks.jmh.pagemem;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.logger.NullLogger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Batch updates in pagemem through preloader.
 *
 * todo benchmark for internal testing purposes.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xms3g", "-Xmx3g", "-server", "-XX:+AggressiveOpts", "-XX:MaxMetaspaceSize=256m"})
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 10)
public class JmhBatchUpdatesInPreloadBenchmark {
    /** */
    private static final long DEF_REG_SIZE = 20 * 1024 * 1024 * 1024L;

    /** */
    private static final int BATCH_SIZE = 500;

    /** */
    private static final String REG_BATCH = "batch-region";

    /** */
    private static final String REG_SINGLE = "single-region";

    /** */
    private static final String CACHE_BATCH = "batch";

    /** */
    private static final String CACHE_SINGLE = "single";

    /** */
    private static final String NODE_NAME = "srv0";

    /** */
    private static int iteration = 0;

    /** */
    public enum OBJECT_SIZE_RANGE {
        /** */
        r0_4(0, 4),

        /** */
        r4_16(4, 16),

        /** */
        r16_64(16, 64),

        /** */
        r100_200(100, 200),

        /** */
        r200_500(200, 500),

        /** */
        r500_800(500, 800),

        /** */
        r800_1200(800, 1200),

        /** */
        r2000_3000(2_000, 3_000),

        /** */
        r1000_8000(1_000, 8_000),

        /** Large objects only. */
        r4000_16000(4_000, 16_000),

        /** Mixed objects, mostly large objects. */
        r100_32000(100, 32_000);

        /** */
        private final int min;

        /** */
        private final int max;

        /** */
        OBJECT_SIZE_RANGE(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }

    /**
     * Create Ignite configuration.
     *
     * @return Ignite configuration.
     */
    private IgniteConfiguration getConfiguration(String cfgName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new NullLogger());

        cfg.setIgniteInstanceName(cfgName);

        DataRegionConfiguration reg1 = new DataRegionConfiguration();
        reg1.setInitialSize(DEF_REG_SIZE);
        reg1.setMaxSize(DEF_REG_SIZE);
        reg1.setName(REG_BATCH);

        DataRegionConfiguration reg2 = new DataRegionConfiguration();
        reg2.setInitialSize(DEF_REG_SIZE);
        reg2.setMaxSize(DEF_REG_SIZE);
        reg2.setName(REG_SINGLE);

        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        storeCfg.setDataRegionConfigurations(reg1, reg2);

        cfg.setDataStorageConfiguration(storeCfg);

        cfg.setCacheConfiguration(ccfg(false), ccfg(true));

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg(boolean batch) {
        return new CacheConfiguration<K, V>(batch ? CACHE_BATCH : CACHE_SINGLE)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setDataRegionName(batch ? REG_BATCH : REG_SINGLE);
    }

    /**
     * Test single updates.
     *
     * @param data Data that will be preloaded.
     * @param preloader Data preloader.
     */
    @Benchmark
    @Fork(jvmArgsAppend = "-D" + IgniteSystemProperties.IGNITE_DATA_STORAGE_BATCH_PAGE_WRITE + "=false")
    public void checkSingle(Data data, Preloader preloader) {
        preloader.demanderSingle.handleSupplyMessage(0, data.node.localNode().id(), data.singleData);
    }

    /**
     * Test batch updates.
     *
     * @param data Data that will be preloaded.
     * @param preloader Data preloader.
     */
    @Benchmark
    @Fork(jvmArgsAppend = "-D" + IgniteSystemProperties.IGNITE_DATA_STORAGE_BATCH_PAGE_WRITE + "=true")
    public void checkBatch(Data data, Preloader preloader) {
        preloader.demanderBatch.handleSupplyMessage(0, data.node.localNode().id(), data.batchData);
    }

    /**
     * Start 2 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() {
        IgniteEx node = (IgniteEx)Ignition.start(getConfiguration(NODE_NAME));

        partitionState(node, CACHE_BATCH, GridDhtPartitionState.MOVING);
        partitionState(node, CACHE_SINGLE, GridDhtPartitionState.MOVING);
    }

    /** */
    private void partitionState(IgniteEx node, String name, GridDhtPartitionState state) {
        for (GridDhtLocalPartition part : node.cachex(name).context().group().topology().localPartitions())
            part.setState(state);
    }

    /**
     * Stop all grids after tests.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * Create streamer on client cache.
     */
    @State(Scope.Benchmark)
    public static class Preloader {
        /** */
        final GridDhtPartitionDemander demanderBatch = demander(CACHE_BATCH);

        /** */
        final GridDhtPartitionDemander demanderSingle = demander(CACHE_SINGLE);

        /** */
        GridDhtPartitionDemander demander(String name) {
            try {
                GridCacheContext cctx = ((IgniteEx)Ignition.ignite(NODE_NAME)).cachex(name).context();
                GridDhtPreloader preloader = (GridDhtPreloader)cctx.group().preloader();
                AffinityTopologyVersion topVer = cctx.group().affinity().lastVersion();

                GridDhtPartitionDemander.RebalanceFuture fut =
                    newInstance(GridDhtPartitionDemander.RebalanceFuture.class);

                setFieldValue(fut, "rebalanceId", 0);
                setFieldValue(fut, "topVer", topVer);

                GridDhtPartitionDemander demander = getFieldValue(preloader, "demander");

                setFieldValue(demander, "rebalanceFut", fut);

                setFieldValue(cctx.shared().exchange(), "rebTopVer", topVer);

                return demander;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Prepare and clean collection with streaming data.
     */
    @State(Scope.Thread)
    public static class Data {
        /** */
        @Param
        private OBJECT_SIZE_RANGE range;

        /** */
        private int[] sizes;

        /** */
        private GridDhtPartitionSupplyMessage batchData;

        /** */
        private GridDhtPartitionSupplyMessage singleData;

        /** */
        private GridCacheContext cctxBatch = ((IgniteEx)Ignition.ignite(NODE_NAME)).cachex(CACHE_BATCH).context();

        /** */
        private GridCacheContext cctxSingle = ((IgniteEx)Ignition.ignite(NODE_NAME)).cachex(CACHE_SINGLE).context();

        /** */
        private IgniteEx node = ((IgniteEx)Ignition.ignite(NODE_NAME));

        /** */
        int part = 0;

        /** */
        @Setup(Level.Trial)
        public void setup() {
            sizes = sizes(range.min, range.max, BATCH_SIZE);
        }

        /**
         * Prepare collection.
         */
        @Setup(Level.Invocation)
        public void prepare() {
            int iter = iteration++;
            int off = iter * BATCH_SIZE;
            int rebalanceId = 0;

            batchData = prepareSupplyMessage(cctxBatch, rebalanceId, part, off, BATCH_SIZE, sizes);
            singleData = prepareSupplyMessage(cctxSingle, rebalanceId, part, off, BATCH_SIZE, sizes);
        }

        /**
         * Clean collection after each test.
         */
        @TearDown(Level.Iteration)
        public void cleanCollection() {
            batchData = null;
            singleData = null;
        }

        /** */
        int[] sizes(int minObjSize, int maxObjSize, int batchSize) {
            int sizes[] = new int[batchSize];
            int minSize = maxObjSize;
            int maxSize = minObjSize;

            int delta = maxObjSize - minObjSize;

            for (int i = 0; i < batchSize; i++) {
                int size = sizes[i] = minObjSize + (delta > 0 ? ThreadLocalRandom.current().nextInt(delta) : 0);

                if (size < minSize)
                    minSize = size;

                if (size > maxSize)
                    maxSize = size;
            }

            return sizes;
        }

        /** */
        private GridDhtPartitionSupplyMessage prepareSupplyMessage(
            GridCacheContext cctx,
            int rebalanceId,
            int p,
            int off,
            int cnt,
            int[] sizes
        ) {
            GridDhtPartitionSupplyMessage msg = newInstance(GridDhtPartitionSupplyMessage.class);

            setFieldValue(msg, GridCacheGroupIdMessage.class, "grpId", cctx.group().groupId());
            setFieldValue(msg, "rebalanceId", rebalanceId);
            setFieldValue(msg, "topVer", cctx.group().affinity().lastVersion());

            List<GridCacheEntryInfo> infos = new ArrayList<>();

            for (int i = off; i < off + cnt; i++) {
                int size = sizes[i - off];

                KeyCacheObject key = cctx.toCacheKeyObject(i);
                CacheObject val = cctx.toCacheObject(new byte[size]);

                GridCacheEntryInfo info = new GridCacheEntryInfo();
                info.key(key);
                info.value(val);
                info.cacheId(cctx.cacheId());
                info.version(cctx.shared().versions().startVersion());

                infos.add(info);
            }

            Map<Object, Object> map = new HashMap<>();

            map.put(p, new CacheEntryInfoCollection(infos));

            setFieldValue(msg, "infos", map);

            return msg;
        }
    }

    /** */
    private static <T> T newInstance(Class<T> clazz) {
        Constructor constructors[] = clazz.getDeclaredConstructors();

        try {
            for (Constructor constructor : constructors) {
                if (constructor.getParameterTypes().length == 0) {
                    constructor.setAccessible(true);

                    return (T)constructor.newInstance();
                }
            }
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        throw new RuntimeException("No default constructor");
    }

    /**
     * Get object field value via reflection.
     *
     * @param obj Object or class to get field value from.
     * @param fieldNames Field names to get value for: obj->field1->field2->...->fieldN.
     * @param <T> Expected field class.
     * @return Field value.
     * @throws IgniteException In case of error.
     */
    private static <T> T getFieldValue(Object obj, String... fieldNames) throws IgniteException {
        assert obj != null;
        assert fieldNames != null;
        assert fieldNames.length >= 1;

        try {
            for (String fieldName : fieldNames) {
                Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

                try {
                    obj = findField(cls, obj, fieldName);
                }
                catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }

            return (T)obj;
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Failed to get object field [obj=" + obj +
                ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);
        }
    }

    /**
     * @param cls Class for searching.
     * @param obj Target object.
     * @param fieldName Field name for search.
     * @return Field from object if it was found.
     */
    private static Object findField(Class<?> cls, Object obj,
        String fieldName) throws NoSuchFieldException, IllegalAccessException {
        // Resolve inner field.
        Field field = cls.getDeclaredField(fieldName);

        boolean accessible = field.isAccessible();

        if (!accessible)
            field.setAccessible(true);

        return field.get(obj);
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteException In case of error.
     */
    private static void setFieldValue(Object obj, String fieldName, Object val) throws IgniteException {
        setFieldValue(obj, obj.getClass(), fieldName, val);
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param cls Class to get field from.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteException In case of error.
     */
    private static void setFieldValue(Object obj, Class cls, String fieldName, Object val) throws IgniteException {
        assert fieldName != null;

        try {
            Field field = cls.getDeclaredField(fieldName);

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            if (isFinal) {
                Field modifiersField = Field.class.getDeclaredField("modifiers");

                modifiersField.setAccessible(true);

                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            }

            field.set(obj, val);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
            .include(JmhBatchUpdatesInPreloadBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
