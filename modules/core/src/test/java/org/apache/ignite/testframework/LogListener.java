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

package org.apache.ignite.testframework;

import java.time.temporal.ValueRange;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class LogListener implements Consumer<String> {

    public abstract void check() throws AssertionError;

    public static LogListenerBuilder matches(String substr) {
        return new LogListenerBuilder().andMatches(substr);
    }

    public static LogListenerBuilder matches(Pattern regexp) {
        return new LogListenerBuilder().andMatches(regexp);
    }

    public static LogListenerBuilder matches(Predicate<String> pred) {
        return new LogListenerBuilder().andMatches(pred);
    }

    public static class LogListenerBuilder {

        private LogListenerBuilder() {};

        /** */
        private final CompositeMessageListener lsnr = new CompositeMessageListener();

        /** */
        private Node prev;

        private void addLast(Node node) {
            if (prev != null)
                lsnr.add(prev.listener());

            prev = node;
        }

        public LogListenerBuilder andMatches(String substr) {
            addLast(new Node(substr, msg -> msg.contains(substr)));

            return this;
        }

        public LogListenerBuilder andMatches(Pattern regexp) {
            addLast(new Node(regexp.toString(), msg -> regexp.matcher(msg).matches()));

            return this;
        }

        public LogListenerBuilder andMatches(Predicate<String> pred) {
            addLast(new Node(null, pred));

            return this;
        }

        public LogListener build() {
            addLast(null);

            return lsnr;
        }

        public LogListenerBuilder times(int n) {
            if (prev != null)
                prev.min = prev.max = n;

            return this;
        }

        public LogListenerBuilder atLeast(int n) {
            if (prev != null)
                prev.min = n;

            return this;
        }

        public LogListenerBuilder atMost(int n) {
            if (prev != null)
                prev.max = n;

            return this;
        }

        public LogListenerBuilder orError(String msg) {
            if (prev != null)
                prev.msg = msg;

            return this;
        }

        /** */
        final class Node {
            /** */
            final String subj;

            /** */
            final Predicate<String> pred;

            /** */
            String msg;

            /** */
            int min = 1;

            /** */
            int max = Integer.MAX_VALUE;

            /** */
            Node(String subj, Predicate<String> pred) {
                this.subj = subj;
                this.pred = pred;
            }

            /** */
            LogMessageListener listener() {
                return new LogMessageListener(pred, ValueRange.of(min, max), subj, msg);
            }
        }

        /** */
        private static class LogMessageListener extends LogListener {
            /** */
            private final Predicate<String> pred;

            /** */
            private final AtomicReference<Throwable> err = new AtomicReference<>();

            /** */
            private final AtomicInteger hits = new AtomicInteger();

            /** */
            private final ValueRange exp;

            /** */
            private final String subj;

            /** */
            private final String errMsg;

            /**
             *
             * @param subj Search subject.
             * @param exp Expected occurrences.
             * @param pred Search predicate.
             * @param errMsg Custom error message.
             */
            protected LogMessageListener(
                @NotNull Predicate<String> pred,
                @NotNull ValueRange exp,
                @Nullable String subj,
                @Nullable String errMsg
            ) {
                this.pred = pred;
                this.exp = exp;
                this.subj = subj == null ? pred.toString() : subj;
                this.errMsg = errMsg;
            }

            /** {@inheritDoc} */
            @Override public void accept(String msg) {
                if (err.get() != null)
                    return;

                try {
                    if (pred.test(msg))
                        hits.incrementAndGet();
                } catch (Throwable t) {
                    err.compareAndSet(null, t);

                    if (t instanceof VirtualMachineError)
                        throw t;
                }
            }

            /** {@inheritDoc} */
            @Override public void check() {
                errCheck();

                int hitsCnt = hits.get();

                if (!exp.isValidIntValue(hitsCnt)) {
                    if (errMsg != null)
                        throw new AssertionError(errMsg);

                    throw new AssertionError("\"" + subj + "\" matches " + hitsCnt +
                        " times, expected " + (exp.getMaximum() == exp.getMinimum() ? exp.getMinimum() : exp) + ".");
                }
            }

            /**
             * Check that there were no runtime errors.
             */
            private void errCheck() {
                Throwable t = err.get();

                if (t instanceof Error)
                    throw (Error) t;

                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;

                assert t == null : t;
            }
        }

        /** */
        private static class CompositeMessageListener extends LogListener {
            /** */
            private final List<LogMessageListener> lsnrs = new ArrayList<>();

            /** {@inheritDoc} */
            @Override public void check() {
                for (LogMessageListener lsnr : lsnrs)
                    lsnr.check();
            }

            /** {@inheritDoc} */
            @Override public void accept(String msg) {
                for (LogMessageListener lsnr : lsnrs)
                    lsnr.accept(msg);
            }


            public void add(LogMessageListener lsnr) {
                lsnrs.add(lsnr);
            }

        }
    }
}
