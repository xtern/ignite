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

package org.apache.ignite.internal.processors.diag;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.diag.DiagnosticTopics.TOTAL;

/**
 * General rebalance diagnostic processing API
 */
public class DiagnosticProcessor extends GridProcessorAdapter {
    /** */
    private final ConcurrentMap<String, LongAdder> timings = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<String, LongAdder> counts = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<String, Long> tracks = new ConcurrentHashMap<>();

    /** */
    private volatile boolean enabled;

    /**
     * @param ctx Context.
     */
    public DiagnosticProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (DiagnosticTopics topics : DiagnosticTopics.values()) {
            timings.put(topics.getName(), new LongAdder());

            counts.put(topics.getName(), new LongAdder());
        }

        U.quietAndInfo(log, "DiagnosticProcessor started");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        resetCounts();
    }

    /** */
    public void beginTrack(DiagnosticTopics topic) {
        beginTrack(topic.getName());
    }

    /** */
    public void endTrack(DiagnosticTopics topic) {
        endTrack(topic.getName());
    }

    /** */
    public synchronized void beginTrack(String topic) {
        if (TOTAL.getName().equals(topic))
            enabled = true;

        if (!enabled)
            return;

        tracks.putIfAbsent(topic, U.currentTimeMillis());
    }

    /** */
    public synchronized void endTrack(String topic) {
        if (!enabled)
            return;

        if (TOTAL.getName().equals(topic))
            enabled = false;

        Long value = tracks.remove(topic);

        if (value == null)
            return;

        timings.get(topic).add(U.currentTimeMillis() - value);
        counts.get(topic).increment();
    }

    /** */
    public synchronized void printStats() {
        Long total = timings.get(TOTAL.getName()).longValue();

        String out = timings.entrySet()
            .stream()
            .filter(e -> e.getValue().longValue() != 0)
            .sorted(Comparator.comparingInt(o -> DiagnosticTopics.get(o.getKey()).ordinal()))
            .map(e -> String.format("# %s : %s ms : %.2f : %s",
                e.getKey(),
                e.getValue().longValue(),
                ((float)e.getValue().longValue() / total * 100),
                counts.get(e.getKey()).longValue()))
            .collect(Collectors.joining("\n"));

        log.info("\n# Diagnostic processor info: \n" + out);

        resetCounts();

        if (!tracks.isEmpty()) {
            String str = tracks.entrySet()
                .stream()
                .map(e -> "# " + e.getKey() + " : " + e.getValue())
                .collect(Collectors.joining("\n"));

            log.info("\n# Unfinished tracks: \n" + str);
        }

        tracks.clear();
    }

    /** */
    public synchronized void resetCounts() {
        for (Map.Entry<String, LongAdder> e : timings.entrySet())
            e.getValue().reset();

        for (Map.Entry<String, LongAdder> c : counts.entrySet())
            c.getValue().reset();
    }
}
