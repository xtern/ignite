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

package org.apache.ignite.testframework.test;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListenerChain;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test.
 */
public class ListeningTestLoggerTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteVersionLogging() throws Exception {
        int gridCnt = 4;

        LogListenerChain r = log.contains(IgniteVersionUtils.VER_STR).listen();

        try {
            startGridsMultiThreaded(gridCnt);

//            assertTrue(r.get() + " < " + gridCnt, r.get() >= gridCnt);
//
//            assertEquals(0, r.get() % gridCnt);
            r.check();
        } finally {
            stopAllGrids();
        }
    }
//
//    /**
//     * Check basic API.
//     */
//    @SuppressWarnings("ThrowableNotThrown")
//    public void testBasicApi() {
//        ListeningTestLogger log = new ListeningTestLogger();
//
//        AtomicBoolean basicMatch = new AtomicBoolean();
//        AtomicBoolean errMatch = new AtomicBoolean();
//        AtomicBoolean stacktraceMatch = new AtomicBoolean();
//
//        log.listen("a[a-z]+", msg -> basicMatch.set(true));
//        log.listen("Exception message.", msg -> errMatch.set(true));
//        log.listen("\\.java:", msg -> stacktraceMatch.set(true));
//
//        log.info("a");
//
//        assertFalse(basicMatch.get());
//
//        log.info("abcdef");
//
//        assertTrue(basicMatch.get());
//
//        assertFalse(errMatch.get());
//        assertFalse(stacktraceMatch.get());
//
//        log.error("There was an error.", new RuntimeException("Exception message."));
//
//        assertTrue(errMatch.get());
//        assertTrue(stacktraceMatch.get());
//
//        log.listen("Ignite!", msg -> {
//            throw new RuntimeException("Test message.");
//        });
//
//        GridTestUtils.assertThrows(null, () -> {
//            log.info("Ignite!");
//
//            return null;
//        }, RuntimeException.class, "Test message.");
//    }
//
//    /**
//     * CHeck thread safety.
//     *
//     * @throws Exception If failed.
//     */
//    public void testMultithreaded() throws Exception {
//        int iterCnt = 50_000;
//        int threadCnt = 6;
//
//        ListeningTestLogger log = new ListeningTestLogger();
//
//        AtomicInteger cntr = new AtomicInteger();
//
//        log.listen("abba", msg -> cntr.incrementAndGet());
//        log.listen("ab", msg -> cntr.incrementAndGet());
//        log.listen("ba", msg -> cntr.incrementAndGet());
//
//        GridTestUtils.runMultiThreaded(() -> {
//            for (int i = 0; i < iterCnt; i++)
//                log.info("It is abba message.");
//        }, threadCnt, "thread-");
//
//        assertEquals(threadCnt * iterCnt * 3, cntr.get());
//    }
//
//    /**
//     * Check "echo" logger.
//     */
//    public void testEchoLogger() {
//        StringBuilder buf = new StringBuilder();
//
//        IgniteLogger echo = new NullLogger() {
//            @Override public void trace(String msg) {
//                buf.append(msg);
//            }
//
//            @Override public void debug(String msg) {
//                buf.append(msg);
//            }
//
//            @Override public void info(String msg) {
//                buf.append(msg);
//            }
//
//            @Override public void warning(String msg, Throwable t) {
//                buf.append(msg);
//            }
//
//            @Override public void error(String msg, Throwable t) {
//                buf.append(msg);
//            }
//        };
//
//        ListeningTestLogger log = new ListeningTestLogger(true, echo);
//
//        log.error("1");
//        log.warning("2");
//        log.info("3");
//        log.debug("4");
//        log.trace("5");
//
//        assertEquals("12345", buf.toString());
//    }
}