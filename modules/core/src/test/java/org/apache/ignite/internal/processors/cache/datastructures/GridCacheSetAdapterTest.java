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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public abstract class GridCacheSetAdapterTest extends IgniteCollectionAbstractTest {
    /** */
    protected static final String SET_NAME = "testSet";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CollectionConfiguration collectionConfiguration() {
        CollectionConfiguration colCfg = super.collectionConfiguration();

        if (colCfg.getCacheMode() == PARTITIONED)
            colCfg.setBackups(1);

        return colCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteCache cache = grid(0).cache(SET_NAME);

        if (cache != null)
            cache.destroy();

        waitSetResourcesCleared();

        super.afterTest();
    }

    /**
     * Waits when internal set maps are cleared.
     *
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private void waitSetResourcesCleared() throws IgniteCheckedException {
        final int MAX_CHECK = 5;

        for (int i = 0; i < MAX_CHECK; i++) {
            try {
                assertSetResourcesCleared();

                return;
            }
            catch (AssertionFailedError e) {
                if (i == MAX_CHECK - 1)
                    throw e;

                log.info("Set resources not cleared, will wait more.");

                U.sleep(1000);
            }
        }
    }

    /**
     * Checks internal set maps are cleared.
     */
    private void assertSetResourcesCleared() {
        assertSetIteratorsCleared();

        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal grid = (IgniteKernal)grid(i);

            for (IgniteCache cache : grid.caches()) {
                CacheDataStructuresManager dsMgr = grid.internalCache(cache.getName()).context().dataStructures();

                Map map = GridTestUtils.getFieldValue(dsMgr, "setsMap");

                assertEquals("Set not removed [grid=" + i + ", map=" + map + ']', 0, map.size());

                map = GridTestUtils.getFieldValue(dsMgr, "setDataMap");

                assertEquals("Set data not removed [grid=" + i + ", cache=" + cache.getName() + ", map=" + map + ']',
                    0,
                    map.size());
            }
        }
    }

    /**
     * Checks internal iterators maps are cleared.
     */
    private void assertSetIteratorsCleared() {
        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal grid = (IgniteKernal) grid(i);

            for (IgniteCache cache : grid.caches()) {
                GridCacheQueryManager queries = grid.internalCache(cache.getName()).context().queries();

                Map map = GridTestUtils.getFieldValue(queries, GridCacheQueryManager.class, "qryIters");

                for (Object obj : map.values())
                    assertEquals("Iterators not removed for grid " + i, 0, ((Map) obj).size());
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            assertNull(set(i, SET_NAME, null));

        CollectionConfiguration colCfg0 = config(false);

        Set<Integer> set0 = set(0, SET_NAME, colCfg0);

        assertNotNull(set0);

        for (int i = 0; i < gridCount(); i++) {
            CollectionConfiguration colCfg = config(false);

            Set<Integer> set = set(i, SET_NAME, colCfg);

            assertNotNull(set);
            assertTrue(set.isEmpty());
            assertEquals(0, set.size());
        }

        grid(0).cache(SET_NAME).destroy();

        awaitPartitionMapExchange();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    for (int i = 0; i < gridCount(); i++) {
                        if (set(i, SET_NAME, null) != null)
                            return false;
                    }

                    return true;
                }
                catch (Exception e) {
                    fail("Unexpected exception: " + e);

                    return true;
                }
            }
        }, 1000);

        for (int i = 0; i < gridCount(); i++)
            assertNull(set(i, SET_NAME, null));
    }

    protected <T> Set<T> set(Ignite node, String name, CollectionConfiguration cfg) {
        IgniteCache<T, ?> cache;
        if (cfg == null)
            cache = node.cache(name);
        else
            cache = node.getOrCreateCache(ccfg(name, cfg));

        return cache == null ? null : cache.asSet();
    }

    private <T> Set<T> set(int idx, String name, CollectionConfiguration cfg) {
        return set(grid(idx), name, cfg);
    }

    private <K, V> CacheConfiguration<K, V> ccfg(String name, CollectionConfiguration cfg) {
        CacheConfiguration<K, V> cacheConfiguration = new CacheConfiguration<>(name);

        if (cfg != null) {
            cacheConfiguration.setAtomicityMode(cfg.getAtomicityMode());
            cacheConfiguration.setCacheMode(cfg.getCacheMode());
            cacheConfiguration.setGroupName(cfg.getGroupName());
            cacheConfiguration.setBackups(cfg.getBackups());
        }

        return cacheConfiguration;
    }

    /**
     * @throws Exception If failed.
     */
    public void testApi() throws Exception {
        CollectionConfiguration colCfg = config(false);

        assertNotNull(set(0, SET_NAME, colCfg));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertNotNull(set);
            assertFalse(set.contains(1));
            assertEquals(0, set.size());
            assertTrue(set.isEmpty());
        }

        // Add, isEmpty.

        assertTrue(set(0, SET_NAME, null).add(1));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertEquals(1, set.size());
            assertFalse(set.isEmpty());
            assertTrue(set.contains(1));

            assertFalse(set.add(1));

            assertFalse(set.contains(100));
        }

        // Remove.

        assertTrue(set(0, SET_NAME, null).remove(1));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertEquals(0, set.size());
            assertTrue(set.isEmpty());

            assertFalse(set.contains(1));

            assertFalse(set.remove(1));
        }

        // Contains all.

        Collection<Integer> col1 = new ArrayList<>();
        Collection<Integer> col2 = new ArrayList<>();

        final int ITEMS = 100;

        for (int i = 0; i < ITEMS; i++) {
            assertTrue(set(i % gridCount(), SET_NAME, null).add(i));

            col1.add(i);
            col2.add(i);
        }

        col2.add(ITEMS);

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertEquals(ITEMS, set.size());
            assertTrue(set.containsAll(col1));
            assertFalse(set.containsAll(col2));
        }

        // To array.

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertArrayContent(set.toArray(), ITEMS);
            assertArrayContent(set.toArray(new Integer[ITEMS]), ITEMS);
        }

        // Remove all.

        Collection<Integer> rmvCol = new ArrayList<>();

        for (int i = ITEMS - 10; i < ITEMS; i++)
            rmvCol.add(i);

        assertTrue(set(0, SET_NAME, null).removeAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertFalse(set.removeAll(rmvCol));

            for (Integer val : rmvCol)
                assertFalse(set.contains(val));

            assertArrayContent(set.toArray(), ITEMS - 10);
            assertArrayContent(set.toArray(new Integer[ITEMS - 10]), ITEMS - 10);
        }

        // Add all.

        assertTrue(set(0, SET_NAME, null).addAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertEquals(ITEMS, set.size());

            assertFalse(set.addAll(rmvCol));

            for (Integer val : rmvCol)
                assertTrue(set.contains(val));
        }

        // Retain all.

        assertTrue(set(0, SET_NAME, null).retainAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertEquals(rmvCol.size(), set.size());

            assertFalse(set.retainAll(rmvCol));

            for (int val = 0; val < 10; val++)
                assertFalse(set.contains(val));

            for (int val : rmvCol)
                assertTrue(set.contains(val));
        }

        // Clear.

        set(0, SET_NAME, null).clear();

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertEquals(0, set.size());
            assertTrue(set.isEmpty());
            assertFalse(set.contains(0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        CollectionConfiguration colCfg = config(false);

        final Set<Integer> set0 = set(0, SET_NAME, colCfg);

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertFalse(set.iterator().hasNext());
        }

        int cnt = 0;

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            for (int j = 0; j < 100; j++)
                assertTrue(set.add(cnt++));
        }

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertSetContent(set, cnt);
        }

        // Try to do not use hasNext.

        Collection<Integer> data = new HashSet<>(cnt);

        Iterator<Integer> iter = set0.iterator();

        for (int i = 0; i < cnt; i++)
            assertTrue(data.add(iter.next()));

        assertFalse(iter.hasNext());

        assertEquals(cnt, data.size());

        for (int i = 0; i < cnt; i++)
            assertTrue(data.contains(i));

        // Iterator for empty set.

        set0.clear();

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertFalse(set.iterator().hasNext());
        }

        // Iterator.remove().

        for (int i = 0; i < 10; i++)
            assertTrue(set0.add(i));

        iter = set0.iterator();

        while (iter.hasNext()) {
            Integer val = iter.next();

            if (val % 2 == 0)
                iter.remove();
        }

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertEquals(i % 2 != 0, set.contains(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorClose() throws Exception {
        CollectionConfiguration colCfg = config(false);

        Set<Integer> set0 = set(0, SET_NAME, colCfg);

        for (int i = 0; i < 5000; i++)
            assertTrue(set0.add(i));

        createIterators(set0);

        GridTestUtils.runGC();

        for (int i = 0; i < 10; i++) {
            try {
                set0.size(); // Trigger weak queue poll.

                assertSetIteratorsCleared();
            }
            catch (AssertionFailedError e) {
                if (i == 9)
                    throw e;

                log.info("Set iterators not cleared, will wait");

                Thread.sleep(500);
            }
        }

        // Check iterators are closed on set remove.

        createIterators(set0);

        int idx = gridCount() > 1 ? 1 : 0;

        grid(idx).cache(SET_NAME).destroy();

        for (int i = 0; i < 10; i++) {
            try {
                assertSetIteratorsCleared();
            }
            catch (AssertionFailedError e) {
                if (i == 9)
                    throw e;

                log.info("Set iterators not cleared, will wait");

                Thread.sleep(500);
            }
        }
    }

    /**
     * @param set Set.
     */
    private void createIterators(Set<Integer> set) {
        for (int i = 0; i < 10; i++) {
            Iterator<Integer> iter = set.iterator();

            assertTrue(iter.hasNext());

            iter.next();

            assertTrue(iter.hasNext());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinsAndLeaves() throws Exception {
        if (collectionCacheMode() == LOCAL)
            return;

        CollectionConfiguration colCfg = config(false);

        Set<Integer> set0 = set(0, SET_NAME, colCfg);

        final int ITEMS = 10_000;

        for (int i = 0; i < ITEMS; i++)
            set0.add(i);

        startGrid(gridCount());

        try {
            Set<Integer> set1 = set(0, SET_NAME, null);

            assertNotNull(set1);

            for (int i = 0; i < gridCount() + 1; i++) {
                Set<Integer> set = set(i, SET_NAME, null);

                assertEquals(ITEMS, set.size());

                assertSetContent(set, ITEMS);
            }
        }
        finally {
            stopGrid(gridCount());
        }

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = set(i, SET_NAME, null);

            assertSetContent(set, ITEMS);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreaded() throws Exception {
        testMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedCollocated() throws Exception {
        if (collectionCacheMode() != PARTITIONED)
            return;

        testMultithreaded(true);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testMultithreaded(final boolean collocated) throws Exception {
        CollectionConfiguration colCfg = config(collocated);

        Set<Integer> set0 = set(0, SET_NAME, colCfg);

        assertNotNull(set0);

        Collection<IgniteInternalFuture> futs = new ArrayList<>();

        final int THREADS_PER_NODE = 5;
        final int KEY_RANGE = 10_000;
        final int ITERATIONS = 3000;

        for (int i = 0; i < gridCount(); i++) {
            final int idx = i;

            futs.add(GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Set<Integer> set = set(idx, SET_NAME, null);

                    assertNotNull(set);

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < ITERATIONS; i++) {
                        switch (rnd.nextInt(4)) {
                            case 0:
                                set.add(rnd.nextInt(KEY_RANGE));

                                break;

                            case 1:
                                set.remove(rnd.nextInt(KEY_RANGE));

                                break;

                            case 2:
                                set.contains(rnd.nextInt(KEY_RANGE));

                                break;

                            case 3:
                                for (Integer val : set)
                                    assertNotNull(val);

                                break;

                            default:
                                fail();
                        }

                        if ((i + 1) % 500 == 0)
                            log.info("Executed iterations: " + (i + 1));
                    }

                    return null;
                }
            }, THREADS_PER_NODE, "testSetMultithreaded"));
        }

        for (IgniteInternalFuture fut : futs)
            fut.get();
    }


//    /**
//     * @throws Exception If failed.
//     */
//    public void testCleanup() throws Exception {
//        testCleanup(false);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCleanupCollocated() throws Exception {
//        testCleanup(true);
//    }

    // todo self cleanup test
//    /**
//     * @param collocated Collocation flag.
//     * @throws Exception If failed.
//     */
//    @SuppressWarnings("WhileLoopReplaceableByForEach")
//    private void testCleanup(boolean collocated) throws Exception {
//        CollectionConfiguration colCfg = config(collocated);
//
//        final Set<Integer> set0 = set(0, SET_NAME, colCfg);
//
//        assertNotNull(set0);
//
//        final Collection<Set<Integer>> sets = new ArrayList<>();
//
//        for (int i = 0; i < gridCount(); i++) {
//            Set<Integer> set = set(i, SET_NAME, null);
//
//            assertNotNull(set);
//
//            sets.add(set);
//        }
//
//        Collection<Integer> items = new ArrayList<>(10_000);
//
//        for (int i = 0; i < 10_000; i++)
//            items.add(i);
//
//        set0.addAll(items);
//
//        assertEquals(10_000, set0.size());
//
//        final AtomicBoolean stop = new AtomicBoolean();
//
//        final AtomicInteger val = new AtomicInteger(10_000);
//
//        IgniteInternalFuture<?> fut;
//
//        try {
//            fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
//                @Override public Object call() throws Exception {
//                    try {
//                        while (!stop.get()) {
//                            for (Set<Integer> set : sets)
//                                set.add(val.incrementAndGet());
//                        }
//                    }
//                    catch (IllegalStateException e) {
//                        log.info("Set removed: " + e);
//                    }
//
//                    return null;
//                }
//            }, 5, "set-add-thread");
//
//            set0.close();
//        }
//        finally {
//            stop.set(true);
//        }
//
//        fut.get();
//
//        int cnt = 0;
//
//        GridCacheContext cctx = GridTestUtils.getFieldValue(set0, "cctx");
//
//        for (int i = 0; i < gridCount(); i++) {
//            GridCacheAdapter cache = grid(i).context().cache().internalCache(cctx.name());
//
//            for (Object e : cache.localEntries(new CachePeekMode[]{CachePeekMode.ALL})) {
//                cnt++;
//
//                log.info("Unexpected entry: " + e);
//            }
//        }
//
//        assertEquals("Found unexpected cache entries", 0, cnt);
//
//        for (final Set<Integer> set : sets) {
//            GridTestUtils.assertThrows(log, new Callable<Void>() {
//                @Override public Void call() throws Exception {
//                    set.add(10);
//
//                    return null;
//                }
//            }, IllegalStateException.class, null);
//        }
//    }

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        final Set<Integer> set = set(0, SET_NAME, config(false));

        assertNotNull(set);

        for (int i = 0; i < 10; i++)
            set.add(i);

        //assertEquals(SET_NAME, set.name());
        Collection<Integer> c = grid(0).compute().broadcast(set::size);

        assertEquals(gridCount(), c.size());

        for (Integer size : c)
            assertEquals((Integer)10, size);
    }

    /**
     * Implementation of ignite data structures internally uses special system caches, need make sure
     * that transaction on these system caches do not intersect with transactions started by user.
     *
     * @throws Exception If failed.
     */
    public void testIsolation() throws Exception {
        CollectionConfiguration colCfg = collectionConfiguration();

        Ignite ignite = grid(0);

        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setName("myCache");
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cfg);

        try {
            Set<Integer> set0 = set(ignite, SET_NAME, colCfg);

            assertNotNull(set0);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(1, 1);

                Collection<Integer> items = new ArrayList<>(100);

                for (int i = 0; i < 100; i++)
                    items.add(i);

                set0.addAll(items);

                tx.rollback();
            }

            assertEquals(0, cache.size());

            assertEquals(0, set0.size());

//            set0.close();
        }
        finally {
            ignite.destroyCache(cfg.getName());
        }
    }

//    /**
//     * Test that sets within the same group and compatible configurations are stored in the same cache.
//     *
//     * @throws Exception If failed.
//     */
//    public void testCacheReuse() throws Exception {
//        Ignite ignite = grid(0);
//
//        CollectionConfiguration colCfg = collectionConfiguration();
//
//        colCfg.setAtomicityMode(ATOMIC);
//        colCfg.setGroupName("grp1");
//
//        IgniteSet set1 = set(ignite, "set1", colCfg);
//        IgniteSet set2 = set(ignite, "set2", colCfg);
//
//        assert cctx(set1).cacheId() == cctx(set2).cacheId();
//
//        colCfg.setAtomicityMode(TRANSACTIONAL);
//
//        IgniteSet set3 = set(ignite, "set3", colCfg);
//        IgniteSet set4 = set(ignite, "set4", colCfg);
//
//        assert cctx(set3).cacheId() == cctx(set4).cacheId();
//        assert cctx(set1).cacheId() != cctx(set3).cacheId();
//        assert cctx(set1).groupId() == cctx(set3).groupId();
//
//        colCfg.setGroupName("gtp2");
//
//        IgniteSet set5 = set(ignite, "set5", colCfg);
//        IgniteSet set6 = set(ignite, "set6", colCfg);
//
//        assert cctx(set5).cacheId() == cctx(set6).cacheId();
//        assert cctx(set1).groupId() != cctx(set5).groupId();
//    }

    /**
     * Tests that basic API works correctly when there are multiple structures in multiple groups.
     *
     * @throws Exception If failed.
     */
    public void _testMultipleStructuresInDifferentGroups() throws Exception {
        Ignite ignite = grid(0);

        CollectionConfiguration cfg1 = collectionConfiguration();
        CollectionConfiguration cfg2 = collectionConfiguration().setGroupName("grp2");

        Set<String> set1 = set(ignite, "set1", cfg1);
        Set<String> set2 = set(ignite, "set2", cfg1);
        Set<String> set3 = set(ignite, "set3", cfg2);
        Set<String> set4 = set(ignite, "set4", cfg2);

        assertTrue(set1.add("a"));
        assertTrue(set2.add("b"));
        assertTrue(set3.add("c"));
        assertTrue(set4.add("d"));

        assertFalse(set1.add("a"));
        assertFalse(set2.add("b"));
        assertFalse(set3.add("c"));
        assertFalse(set4.add("d"));

        assertTrue(set1.contains("a"));
        assertTrue(set2.contains("b"));
        assertTrue(set3.contains("c"));
        assertTrue(set4.contains("d"));

        assertEquals(1, set1.size());
        assertEquals(1, set2.size());
        assertEquals(1, set3.size());
        assertEquals(1, set4.size());

        assertFalse(set1.remove("z"));
        assertFalse(set2.remove("z"));
        assertFalse(set3.remove("z"));
        assertFalse(set4.remove("z"));

        assertTrue(set1.remove("a"));
        assertTrue(set2.remove("b"));
        assertTrue(set3.remove("c"));
        assertTrue(set4.remove("d"));

        assertTrue(set1.isEmpty());
        assertTrue(set2.isEmpty());
        assertTrue(set3.isEmpty());
        assertTrue(set4.isEmpty());

//        set2.close();
//        set4.close();

//        assertTrue(set2.removed());
//        assertTrue(set4.removed());

//        assertFalse(set1.removed());
//        assertFalse(set3.removed());

        assertNotNull(set(ignite, "set1", null));
        assertNull(set(ignite, "set2", null));

//        set1.close();
//        set3.close();
    }

    /**
     * @param set Set.
     * @param size Expected size.
     */
    private void assertSetContent(Set<Integer> set, int size) {
        Collection<Integer> data = new HashSet<>(size);

        for (Integer val : set)
            assertTrue(data.add(val));

        assertEquals(size, data.size());

        for (int val = 0; val < size; val++)
            assertTrue(data.contains(val));
    }

    /**
     * @param arr Array.
     * @param size Expected size.
     */
    private void assertArrayContent(Object[] arr, int size) {
        assertEquals(size, arr.length);

        for (int i = 0; i < size; i++) {
            boolean found = false;

            for (Object obj : arr) {
                if (obj.equals(i)) {
                    found = true;

                    break;
                }
            }

            assertTrue(found);
        }
    }
}
