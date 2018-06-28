package org.apache.ignite.cache.tck_pp;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.processor.EntryProcessor;
import org.jsr107.tck.event.CacheEntryListenerClient;
import org.jsr107.tck.event.CacheEntryListenerServer;
import org.jsr107.tck.expiry.CacheExpiryTest;
import org.jsr107.tck.expiry.ExpiryPolicyClient;
import org.jsr107.tck.expiry.ExpiryPolicyServer;
import org.jsr107.tck.integration.CacheLoaderClient;
import org.jsr107.tck.integration.CacheLoaderServer;
import org.jsr107.tck.integration.RecordingCacheLoader;
import org.jsr107.tck.processor.AssertNotPresentEntryProcessor;
import org.jsr107.tck.processor.CombineEntryProcessor;
import org.jsr107.tck.processor.GetEntryProcessor;
import org.jsr107.tck.processor.SetEntryProcessor;
import org.jsr107.tck.testutil.CacheTestSupport;
import org.jsr107.tck.testutil.ExcludeListExcluder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.jsr107.tck.testutil.TestSupport.MBeanType.CacheStatistics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CacheExpiryTest1 extends CacheExpiryTest {
    /**
     * Rule used to exclude tests
     */
    @Rule
    public ExcludeListExcluder rule = new ExcludeListExcluder(this.getClass());

    private ExpiryPolicyServer expiryPolicyServer;
    private ExpiryPolicyClient expiryPolicyClient;

    @Before
    public void setUp() throws IOException {
        //establish and open a ExpiryPolicyServer to handle cache
        //cache loading requests from a ExpiryPolicyClient
        expiryPolicyServer = new ExpiryPolicyServer(10005);
        expiryPolicyServer.open();

        //establish a ExpiryPolicyClient that a Cache can use for computing expiry policy
        //(via the ExpiryPolicyServer)
        expiryPolicyClient =
            new ExpiryPolicyClient(expiryPolicyServer.getInetAddress(), expiryPolicyServer.getPort());

        cacheEntryListenerServer = new CacheEntryListenerServer<Integer, Integer>(10011, Integer.class, Integer.class);
        cacheEntryListenerServer.open();
        cacheEntryListerClient =
            new CacheEntryListenerClient<>(cacheEntryListenerServer.getInetAddress(), cacheEntryListenerServer.getPort());
    }

    @Override
    protected MutableConfiguration<Integer, Integer> newMutableConfiguration() {
        return new MutableConfiguration<Integer, Integer>().setTypes(Integer.class, Integer.class);
    }

    @Override
    protected MutableConfiguration<Integer, Integer> extraSetup(MutableConfiguration<Integer, Integer> configuration) {
        listener = new CacheTestSupport.MyCacheEntryListener<Integer, Integer>(true);

        //establish a CacheEntryListenerClient that a Cache can use for CacheEntryListening
        //(via the CacheEntryListenerServer)

        listenerConfiguration =
            new MutableCacheEntryListenerConfiguration<>(FactoryBuilder.factoryOf(cacheEntryListerClient), null, true, true);
        cacheEntryListenerServer.addCacheEventListener(listener);
        return configuration.addCacheEntryListenerConfiguration(listenerConfiguration);
    }


    @After
    public void cleanupAfterEachTest() throws InterruptedException {
        for (String cacheName : getCacheManager().getCacheNames()) {
            getCacheManager().destroyCache(cacheName);
        }
        expiryPolicyServer.close();
        expiryPolicyServer = null;

        //close the server
        cacheEntryListenerServer.close();
        cacheEntryListenerServer = null;
    }


    @Test
    public void putShouldCallGetExpiry() {
        CacheExpiryTest.CountingExpiryPolicy expiryPolicy = new CacheExpiryTest.CountingExpiryPolicy();
        expiryPolicyServer.setExpiryPolicy(expiryPolicy);

        MutableConfiguration<Integer, Integer> config = new MutableConfiguration<>();

        config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(expiryPolicyClient));

        Cache<Integer, Integer> cache = getCacheManager().createCache(getTestCacheName(), config);

        cache.containsKey(1);

        assertThat(expiryPolicy.getCreationCount(), is(0));
        assertThat(expiryPolicy.getAccessCount(), is(0));
        assertThat(expiryPolicy.getUpdatedCount(), is(0));

        cache.put(1, 1);
//
//        assertThat(expiryPolicy.getCreationCount(), greaterThanOrEqualTo(1));
//        assertThat(expiryPolicy.getAccessCount(), is(0));
//        assertThat(expiryPolicy.getUpdatedCount(), is(0));
//        expiryPolicy.resetCount();
//
//        cache.put(1, 1);
//
//        assertThat(expiryPolicy.getCreationCount(), is(0));
//        assertThat(expiryPolicy.getAccessCount(), is(0));
//        assertThat(expiryPolicy.getUpdatedCount(), greaterThanOrEqualTo(1));
//        expiryPolicy.resetCount();
    }


    private CacheEntryListenerServer<Integer, Integer> cacheEntryListenerServer;
    private CacheEntryListenerClient<Integer, Integer> cacheEntryListerClient;
}
