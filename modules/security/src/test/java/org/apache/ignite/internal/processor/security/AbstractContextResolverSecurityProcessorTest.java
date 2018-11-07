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

package org.apache.ignite.internal.processor.security;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class AbstractContextResolverSecurityProcessorTest extends AbstractSecurityTest {
    /** Cache name for tests. */
    protected static final String CACHE_WITH_PERMS = "TEST_CACHE";

    /** Cache name for tests. */
    protected static final String CACHE_WITHOUT_PERMS = "SECOND_TEST_CACHE";

    /** Values. */
    protected AtomicInteger values = new AtomicInteger(0);

    /** Sever node that has all permissions. */
    protected IgniteEx srvAllPerms;

    /** Client node that has all permissions. */
    protected IgniteEx clntAllPerms;

    /** Sever node that has read only permission for TEST_CACHE. */
    protected IgniteEx srvReadOnlyPerm;

    /** Client node that has read only permission for TEST_CACHE. */
    protected IgniteEx clntReadOnlyPerm;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srvAllPerms = startGrid("user_0", builder().build());

        clntAllPerms = startGrid("user_1", builder().build(), true);

        srvReadOnlyPerm = startGrid("user_2",
            builder().appendCachePermissions(CACHE_WITH_PERMS, CACHE_READ).build());

        clntReadOnlyPerm = startGrid("user_3",
            builder().appendCachePermissions(CACHE_WITH_PERMS, CACHE_READ).build(), true);

        grid(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(getCacheConfigurations());
    }

    /**
     * Getting array of cache configurations.
     */
    protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_WITH_PERMS)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false),
            new CacheConfiguration<>()
                .setName(CACHE_WITHOUT_PERMS)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
        };
    }

    /**
     * Getting the key that is contained on primary partition on passed node.
     *
     * @param ignite Node.
     * @return Key.
     */
    protected Integer primaryKey(IgniteEx ignite) {
        Affinity<Integer> affinity = ignite.affinity(CACHE_WITHOUT_PERMS);

        int i = 0;
        do {
            if (affinity.isPrimary(ignite.localNode(), ++i))
                return i;

        }
        while (i <= 1_000);

        throw new IllegalStateException(ignite.name() + " isn't primary node for any key.");
    }

    /**
     * Assert that the passed throwable contains a cause exception with given type and message.
     *
     * @param throwable Throwable.
     */
    protected void assertCauseMessage(Throwable throwable) {
        assertThat(X.cause(throwable, SecurityException.class), notNullValue());
    }
}