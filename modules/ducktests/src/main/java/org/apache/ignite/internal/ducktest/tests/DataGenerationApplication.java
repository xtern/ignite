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

package org.apache.ignite.internal.ducktest.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** */
    private static class Streamer implements Runnable {
        /** Cache name. */
        private final String cacheName;

        /** Size. */
        private final int cnt;

        /** Start index. */
        private final int startIdx;

        /** Ignite. */
        private final Ignite ignite;

        /**
         * @param ignite Ignite.
         * @param cacheName Cache name.
         * @param startIdx Start index.
         * @param cnt Batch size.
         */
        public Streamer(Ignite ignite, String cacheName, int startIdx, int cnt) {
            this.cacheName = cacheName;
            this.cnt = cnt;
            this.startIdx = startIdx;
            this.ignite = ignite;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer(cacheName)) {
                for (int i = startIdx; i < startIdx + cnt; i++) {
                    stmr.addData(i, "streamed data-" + i);

                    if (i % 10_000 == 0)
                        log.info("Streamed " + i + " entries");
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        log.info("Creating cache...");

        String cacheName = jsonNode.get("cacheName").asText();

        JsonNode threadsCntNode = jsonNode.get("threads_count");

        int threadCnt = threadsCntNode == null ? 1 : threadsCntNode.asInt();

        ignite.getOrCreateCache(cacheName);

        int maxOps = jsonNode.get("range").asInt() / threadCnt;

        List<Thread> threads = new ArrayList<>(threadCnt - 1);

        int perThreadOps = maxOps / threadCnt;

        for (int i = 0; i < threadCnt; i++) {
            Streamer streamer = new Streamer(ignite, cacheName, i * perThreadOps, perThreadOps);

            if (i == threadCnt - 1) {
                streamer.run();

                break;
            }

            Thread t = new Thread(streamer, "stream-" + i);

            threads.add(t);

            t.start();
        }

        for (int i = 0; i < threads.size(); i++) {
            try {
                threads.get(i).join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        markSyncExecutionComplete();
    }
}
