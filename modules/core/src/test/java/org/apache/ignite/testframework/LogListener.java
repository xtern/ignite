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

/**
 * Log messages listener.
 */
public abstract class LogListener implements Consumer<String> {
    /**
     * Checks that all conditions are met.
     *
     * @throws AssertionError If some condition failed.
     */
    public abstract void check() throws AssertionError;

    /**
     * Reset listener state.
     */
    abstract void reset();

    /**
     * Creates new listener builder.
     *
     * @param substr Substring to search for in a log message.
     * @return Log message listener builder.
     */
    public static Builder matches(String substr) {
        return new Builder().andMatches(substr);
    }

    /**
     * Creates new listener builder.
     *
     * @param regexp Regular expression to search for in a log message.
     * @return Log message listener builder.
     */
    public static Builder matches(Pattern regexp) {
        return new Builder().andMatches(regexp);
    }

    /**
     * Creates new listener builder.
     *
     * @param pred Log message predicate.
     * @return Log message listener builder.
     */
    public static Builder matches(Predicate<String> pred) {
        return new Builder().andMatches(pred);
    }

    /**
     * Log listener builder.
     */
    public static class Builder {
        /** */
        private final CompositeMessageListener lsnr = new CompositeMessageListener();

        /** */
        private Node prev;

        /**
         * Add new substring predicate.
         *
         * @param substr Substring.
         * @return current builder instance.
         */
        public Builder andMatches(String substr) {
            addLast(new Node(substr, msg -> msg.contains(substr)));

            return this;
        }

        /**
         * Add new regular expression predicate.
         *
         * @param regexp Regular expressiuo.
         * @return current builder instance.
         */
        public Builder andMatches(Pattern regexp) {
            addLast(new Node(regexp.toString(), msg -> regexp.matcher(msg).find()));

            return this;
        }

        /**
         * Add new log message predicate.
         *
         * @param pred Log message predcate.
         * @return current builder instance.
         */
        public Builder andMatches(Predicate<String> pred) {
            addLast(new Node(null, pred));

            return this;
        }

        /**
         * Set expected number of matches.
         *
         * @param n Expected number of matches.
         * @return current builder instance.
         */
        public Builder times(int n) {
            if (prev != null)
                prev.cnt = n;

            return this;
        }

        /**
         * Set expected minimum number of matches.
         *
         * @param n Expected number of matches.
         * @return current builder instance.
         */
        public Builder atLeast(int n) {
            if (prev != null) {
                prev.min = n;

                prev.cnt = null;
            }

            return this;
        }

        /**
         * Set expected maximum number of matches.
         *
         * @param n Expected number of matches.
         * @return current builder instance.
         */
        public Builder atMost(int n) {
            if (prev != null) {
                prev.max = n;

                prev.cnt = null;
            }

            return this;
        }

        /**
         * Set custom message for assertion error.
         *
         * @param msg Custom message.
         * @return current builder instance.
         */
        public Builder orError(String msg) {
            if (prev != null)
                prev.msg = msg;

            return this;
        }

        /**
         * Constructs message listener.
         *
         * @return Log message listener.
         */
        public LogListener build() {
            addLast(null);

            return lsnr;
        }

        /**
         * @param node Log listener attributes.
         */
        private void addLast(Node node) {
            if (prev != null)
                lsnr.add(prev.listener());

            prev = node;
        }

        /** */
        private Builder() {};

        /**
         * Mutable attributes for log listener.
         */
        static final class Node {
            /** */
            final String subj;

            /** */
            final Predicate<String> pred;

            /** */
            String msg;

            /** */
            Integer min;

            /** */
            Integer max;

            /** */
            Integer cnt;

            /** */
            Node(String subj, Predicate<String> pred) {
                this.subj = subj;
                this.pred = pred;
            }

            /** */
            LogMessageListener listener() {
                ValueRange range;

                if (cnt != null)
                    range = ValueRange.of(cnt, cnt);
                else if (min == null && max == null)
                    range = ValueRange.of(1, Integer.MAX_VALUE);
                else
                    range = ValueRange.of(min == null ? 0 : min, max == null ? Integer.MAX_VALUE : max);

                return new LogMessageListener(pred, range, subj, msg);
            }
        }
    }

    /** */
    private static class LogMessageListener extends LogListener {
        /** */
        private final Predicate<String> pred;

        /** */
        private final AtomicReference<Throwable> err = new AtomicReference<>();

        /** */
        private final AtomicInteger matches = new AtomicInteger();

        /** */
        private final ValueRange exp;

        /** */
        private final String subj;

        /** */
        private final String errMsg;

        /**
         * @param subj Search subject.
         * @param exp Expected occurrences.
         * @param pred Search predicate.
         * @param errMsg Custom error message.
         */
        private LogMessageListener(
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
                    matches.incrementAndGet();
            } catch (Throwable t) {
                err.compareAndSet(null, t);

                if (t instanceof VirtualMachineError)
                    throw t;
            }
        }

        /** {@inheritDoc} */
        @Override public void check() {
            errCheck();

            int matchesCnt = matches.get();

            if (!exp.isValidIntValue(matchesCnt)) {
                String err =  errMsg != null ? errMsg :
                    "\"" + subj + "\" matches " + matchesCnt + " times, expected: " +
                        (exp.getMaximum() == exp.getMinimum() ? exp.getMinimum() : exp) + ".";

                throw new AssertionError(err);
            }
        }

        /** {@inheritDoc} */
        @Override void reset() {
            matches.set(0);
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
        @Override void reset() {
            for (LogMessageListener lsnr : lsnrs)
                lsnr.reset();
        }

        /** {@inheritDoc} */
        @Override public void accept(String msg) {
            for (LogMessageListener lsnr : lsnrs)
                lsnr.accept(msg);
        }

        /**
         * @param lsnr Listener.
         */
        private void add(LogMessageListener lsnr) {
            lsnrs.add(lsnr);
        }
    }
}
