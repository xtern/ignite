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

package org.apache.ignite.internal.processors.cache;

import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

//import static javax.cache.configuration.FactoryBuilder.factoryOf;

/** */
public class GridCacheResourcesCleanupTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** */
    private static final String DFLT_CACHE = "cache1";

    /** */
    private static final List<CloseableResource> refs = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (grid(0).cache(DFLT_CACHE) != null)
            grid(0).cache(DFLT_CACHE).destroy();

        refs.clear();
    }

    /** */
    public void testCacheStoreSessionListenerCleanup() throws Exception {
        CloseableCacheStoreSessionListener lsnr1 = new CloseableCacheStoreSessionListener();
        CloseableCacheStoreSessionListener lsnr2 = new CloseableCacheStoreSessionListener();

        testResourcesCleanup(cfg().setCacheStoreSessionListenerFactories(factoryOf(lsnr1), factoryOf(lsnr2)));
    }

    /** */
    public void testCacheWriterCleanup() throws Exception {
        testResourcesCleanup(cfg().setCacheWriterFactory(factoryOf(new CloseableCacheWriter<>())).setWriteThrough(true));
    }

    /** */
    public void testCacheLoaderCleanup() throws Exception {
        testResourcesCleanup(cfg().setCacheLoaderFactory(factoryOf(new CloseableCacheLoader<>())));
    }

    /** */
    public void testEvictPolicyCleanup() throws Exception {
        testResourcesCleanup(cfg().setEvictionPolicyFactory(factoryOf(new CloseableEvictionPolicy<>()))
            .setOnheapCacheEnabled(true));
    }

    /** */
    public void testCacheStoreCleanup() throws Exception {
        testResourcesCleanup(cfg().setCacheStoreFactory(factoryOf(new CloseableCacheStore<>())));
    }

    /** */
    public void testContinuousQueryRemoteFilterCleanup() throws Exception {
        Ignite node = grid(0);

        IgniteCache<Integer, String> cache = node.createCache(cfg());

        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

        qry.setLocalListener((evts) -> {});

        qry.setRemoteFilterFactory(factoryOf(new CloseableRemoteFilter<>()));

        assertEquals(0, refs.size());

        cache.query(qry);

//        try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
//        }

        node.destroyCache(DFLT_CACHE);

        assertTrue("No objects was created.", refs.size() > 0);

        for (CloseableResource obj : refs)
            assertTrue("Was not closed: " + obj, obj.closed());
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void testResourcesCleanup(CacheConfiguration<Integer, String> ccfg) throws InterruptedException {
        Ignite node = grid(0);

        assertEquals(0, refs.size());

        IgniteCache<Integer, String> cache = node.createCache(ccfg);

        cache.put(1, "1");
        cache.put(2, "2");

        node.destroyCache(DFLT_CACHE);

        awaitPartitionMapExchange();

        assertTrue("No objects was created.", refs.size() > 0);

        for (CloseableResource obj : refs)
            assertTrue("Was not closed: " + obj, obj.closed());
    }


    /** */
    private <T extends CloseableResource> Factory<T> factoryOf(T obj) {
        return new SingletonFactory<>(obj);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration<Integer, String> cfg() {
        return new CacheConfiguration<>(DFLT_CACHE);
    }

    /** */
    static class SingletonFactory<T extends CloseableResource> implements Factory<T> {
        /** */
        private final T instance;

        /** */
        private SingletonFactory(T instance) {
            this.instance = instance;
        }

        /** {@inheritDoc} */
        @Override public T create() {
            refs.add(instance);

//            U.dumpStack("Created " + instance);

            return instance;
        }
    }

    /** */
    static abstract class CloseableResource implements Closeable, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private volatile boolean closed;

        /** {@inheritDoc} */
        @Override public void close() {
//            U.dumpStack("close resource: " + this);

            assert !closed : closed;



            closed = true;
        }

        /**
         * @return Close counter.
         */
        boolean closed() {
            return closed;
        }
    }

    /** */
    static class CloseableCacheWriter<K, V> extends CloseableResource implements CacheWriter<K, V> {
        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection keys) throws CacheWriterException {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection coll) throws CacheWriterException {
            // no-op
        }
    }

    /** */
    static class CloseableCacheLoader<k, V> extends CloseableResource implements CacheLoader<k, V> {
        /** {@inheritDoc} */
        @Override public V load(k key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<k, V> loadAll(Iterable<? extends k> keys) throws CacheLoaderException {
            return null;
        }
    }

    /** */
    static class CloseableEvictionPolicy<K, V> extends CloseableResource implements EvictionPolicy<K, V> {
        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
            // no-op
        }
    }

    /** */
    static class CloseableCacheStore<k, V> extends CloseableResource implements CacheStore<k, V> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<k, V> clo, @Nullable Object... args) throws CacheLoaderException {

        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {

        }

        /** {@inheritDoc} */
        @Override public V load(k key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<k, V> loadAll(Iterable<? extends k> keys) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends k, ? extends V> entry) throws CacheWriterException {
        }

        /** {@inheritDoc} */
        @Override public void writeAll(
            Collection<Cache.Entry<? extends k, ? extends V>> entries) throws CacheWriterException {
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        }
    }

    /** */
    static class CloseableCacheStoreSessionListener extends CloseableResource implements CacheStoreSessionListener {
        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {}

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {}
    }

    /** */
    static class CloseableRemoteFilter<K, V> extends CloseableResource implements CacheEntryEventFilter<K, V> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent ignore) throws CacheEntryListenerException {
            return true;
        }
    }
}
