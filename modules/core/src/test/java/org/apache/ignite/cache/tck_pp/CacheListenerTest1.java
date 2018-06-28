package org.apache.ignite.cache.tck_pp;

import java.util.Arrays;
import java.util.Collection;
import org.jsr107.tck.event.CacheListenerTest;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class CacheListenerTest1 extends CacheListenerTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {Boolean.FALSE}
        });
    }

    @Test
    @Override public void testCacheEntryListener() {
        super.testCacheEntryListener();
    }
}
