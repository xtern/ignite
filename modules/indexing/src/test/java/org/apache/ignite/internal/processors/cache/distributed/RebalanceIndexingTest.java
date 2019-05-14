package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

public class RebalanceIndexingTest extends GridCommonAbstractTest {

    private static final boolean IDX = false;

    private static final boolean PERSISTENCE = true;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(PERSISTENCE)
                )
        );

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
//        cleanPersistenceDir();
    }

    @Test
//    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testRebalancing() throws Exception {
        Ignite node = startGrid(0);

        node.cluster().active(true);

        node.cluster().baselineAutoAdjustTimeout(0);

        IgniteCache<String, Organization> orgCache = jcache(String.class, Organization.class);

        assert orgCache != null;

        IgniteCache<?, ?> c = jcache(AffinityKey.class, Person.class);
        IgniteCache<AffinityKey<String>, Person>  personCache = (IgniteCache<AffinityKey<String>, Person>)c;

        log.info("Loading data.");

        for (int i = 0; i < 100; i++) {
            String orgKey = "organization" + i;

            orgCache.put(orgKey, new Organization(i, String.valueOf(i)));

            for (int j = 0; j < 10_000; j++) {
                personCache.put(
                    new AffinityKey<>("person" + j, orgKey),
                    new Person("John White " + " " + i + " " + j, 25, i, "Person Person " + j));
            }
        }

        log.info("Data loaded.");

        long startTime = U.currentTimeMillis();

        Ignite node2 = startGrid(1);

        boolean ok = GridTestUtils.waitForCondition(() -> node2.cluster().nodes().size() == 2, 1_000);

        assert ok;

        while (true) {
            try {
                awaitPartitionMapExchange();

                break;
            }
            catch (RuntimeException ignore) {

            }
        }

        System.out.println("Rebalance total time: " + (U.currentTimeMillis() - startTime) + " ms");
    }



    /**
     * Person.
     */
    private static class Person implements Serializable {
        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /** Age. */
        @QuerySqlField(index = IDX)
        private final int age;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final int orgId;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private final String someKey;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private final String someKey1;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey2;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey3;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey4;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey5;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey6;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey7;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey8;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey9;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey10;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey11;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey12;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey13;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey14;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey15;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey16;

        /** Organization ID. */
        @QuerySqlField(index = IDX)
        private final String someKey17;

        /**
         * @param name Name.
         * @param age Age.
         * @param orgId Organization ID.
         * @param someKey
         */
        private Person(String name, int age, int orgId, String someKey) {
            assert !F.isEmpty(name);
            assert age > 0;
            assert orgId >= 0;

            this.name = name;
            this.age = age;
            this.orgId = orgId;
            this.someKey = someKey1 = someKey2 = someKey3 = someKey4 = someKey5 = someKey6 = someKey7 = someKey8 = someKey9 = someKey10 = someKey11 = someKey12 = someKey13 = someKey14 = someKey15 = someKey16 = someKey17 = someKey;
        }

//        /** {@inheritDoc} */
//        @Override public boolean equals(Object o) {
//            if (this == o)
//                return true;
//
//            if (o == null || getClass() != o.getClass())
//                return false;
//
//            Person person = (Person)o;
//
//            return age == person.age && orgId == person.orgId && name.equals(person.name);
//
//        }

//        /** {@inheritDoc} */
//        @Override public int hashCode() {
//            int res = name.hashCode();
//
//            res = 31 * res + age;
//            res = 31 * res + orgId;
//
//            return res;
//        }
    }

    /**
     * Organization.
     */
    private static class Organization implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Organization(int id, String name) {
            assert id >= 0;
            assert !F.isEmpty(name);

            this.id = id;
            this.name = name;
        }
//
//        /** {@inheritDoc} */
//        @Override public boolean equals(Object o) {
//            if (this == o)
//                return true;
//
//            if (o == null || getClass() != o.getClass())
//                return false;
//
//            Organization that = (Organization)o;
//
//            return id == that.id && name.equals(that.name);
//
//        }
//
//        /** {@inheritDoc} */
//        @Override public int hashCode() {
//            int res = id;
//
//            res = 31 * res + name.hashCode();
//
//            return res;
//        }
    }

    /**
     * @param clsK Class k.
     * @param clsV Class v.
     */
    protected <K, V> IgniteCache<K, V> jcache(Class<K> clsK, Class<V> clsV) {
        return jcache(grid(0), cacheConfiguration(), clsK, clsV);
    }

    /**
     * @param name Name.
     * @param clsK Class k.
     * @param clsV Class v.
     */
    protected <K, V> IgniteCache<K, V> jcache(String name, Class<K> clsK, Class<V> clsV) {
        return jcache(grid(0), cacheConfiguration(), name, clsK, clsV);
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
//        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

//        if (cacheMode() == PARTITIONED)
            ccfg.setBackups(1);

        return ccfg;
    }

    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(15);
    }
}
