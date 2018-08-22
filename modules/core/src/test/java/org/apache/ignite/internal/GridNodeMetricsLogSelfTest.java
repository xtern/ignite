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

package org.apache.ignite.internal;

import java.io.StringWriter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;

/**
 * Check logging local node metrics
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal")
public class GridNodeMetricsLogSelfTest extends GridCommonAbstractTest {
    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_0 = "Custom executor 0";

    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_1 = "Custom executor 1";

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMetricsLogFrequency(1000);

        cfg.setExecutorConfiguration(new ExecutorConfiguration(CUSTOM_EXECUTOR_0),
            new ExecutorConfiguration(CUSTOM_EXECUTOR_1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeMetricsLog() throws Exception {
        // Log to string, to check log content
        Layout layout = new SimpleLayout();

        StringWriter strWr = new StringWriter();

        WriterAppender app = new WriterAppender(layout, strWr);

        Logger.getRootLogger().addAppender(app);

        Ignite g1 = startGrid(1);

        IgniteCache<Integer, String> cache1 = g1.createCache("TestCache1");

        cache1.put(1, "one");

        Ignite g2 = startGrid(2);

        IgniteCache<Integer, String> cache2 = g2.createCache("TestCache2");

        cache2.put(2, "two");

        Thread.sleep(10_000);

        //Check that nodes are alie
        assertEquals("one", cache1.get(1));
        assertEquals("two", cache2.get(2));

        String fullLog = strWr.toString();

        Logger.getRootLogger().removeAppender(app);

        String msg = "Metrics are missing in the log or have an unexpected format";

        // don't check the format strictly, but check that all expected metrics are present
        assertTrue(msg, fullLog.contains("Metrics for local node (to disable set 'metricsLogFrequency' to 0)"));
        assertTrue(msg, fullLog.matches("(?s).*Node \\[id=.*, name=.*, uptime=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*H/N/C \\[hosts=.*, nodes=.*, CPUs=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*CPU \\[cur=.*, avg=.*, GC=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*PageMemory \\[pages=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Heap \\[used=.*, free=.*, comm=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Off-heap .+ \\[used=.*, free=.*, comm=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Outbound messages queue \\[size=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*Public thread pool \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*System thread pool \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*" + CUSTOM_EXECUTOR_0 + " \\[active=.*, idle=.*, qSize=.*].*"));
        assertTrue(msg, fullLog.matches("(?s).*" + CUSTOM_EXECUTOR_1 + " \\[active=.*, idle=.*, qSize=.*].*"));
    }
}
