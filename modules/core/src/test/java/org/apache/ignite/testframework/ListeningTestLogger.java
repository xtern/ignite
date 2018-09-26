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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.fail;

/**
 * Implementation of {@link org.apache.ignite.IgniteLogger} that performs any actions when certain message is logged.
 * It can be useful in tests to ensure that a specific message was (or was not) printed to the log.
 */
public class ListeningTestLogger implements IgniteLogger {
    /**
     * If set to {@code true}, enables debug and trace log messages processing.
     */
    private final boolean dbg;

    /**
     * Logger to echo all messages, limited by {@code dbg} flag.
     */
    private final IgniteLogger echo;

    /**
     * Log messages listeners.
     */
    private final List<IgniteInClosure<String>> lsnrs = new CopyOnWriteArrayList<>();

    /**
     * Default constructor.
     */
    public ListeningTestLogger() {
        this(false);
    }

    /**
     * @param dbg If set to {@code true}, enables debug and trace log messages processing.
     */
    public ListeningTestLogger(boolean dbg) {
        this(dbg, null);
    }

    /**
     * @param dbg If set to {@code true}, enables debug and trace log messages processing.
     * @param echo Logger to echo all messages, limited by {@code dbg} flag.
     */
    public ListeningTestLogger(boolean dbg, @Nullable IgniteLogger echo) {
        this.dbg = dbg;
        this.echo = echo;
    }

    /**
     * Register log message listener, that will be executed when certain pattern appears in a log message.
     *
     * @param substr todo
     * @throws PatternSyntaxException If the expression's syntax is invalid.
     */
    public LogListenerChainBuilder contains(@NotNull String substr) {
        return new LogListenerChainBuilderImpl().andContains(substr);
    }

    /**
     * Register log message listener, that will be executed when certain pattern appears in a log message.
     *
     * @param regexp todo
     * @throws PatternSyntaxException If the expression's syntax is invalid.
     */
    public LogListenerChainBuilder match(@NotNull String regexp) {
        return new LogListenerChainBuilderImpl().andMatch(regexp);
    }

    /**
     * todo
     * Register log message listener, that will be executed when certain pattern appears in a log message.
     *
     * @param pred Listener to execute when {@code regex} expression occurs in a log message.
     * @throws PatternSyntaxException If the expression's syntax is invalid.
     */
    public LogListenerChainBuilder filter(@NotNull IgnitePredicate<String> pred) {
        return new LogListenerChainBuilderImpl().andFilter(pred);
    }

    /**
     *
     * @param lsnr Message listener.
     */
    public void listen(@NotNull Consumer<String> lsnr) {
        lsnrs.add(lsnr);
    }

    /**
     * Clears all listeners.
     */
    public void reset() {
        lsnrs.clear();
    }

    /** {@inheritDoc} */
    @Override public ListeningTestLogger getLogger(Object ctgr) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (!dbg)
            return;

        if (echo != null)
            echo.trace(msg);

        applyListeners(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (!dbg)
            return;

        if (echo != null)
            echo.debug(msg);

        applyListeners(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (echo != null)
            echo.info(msg);

        applyListeners(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable t) {
        if (echo != null)
            echo.warning(msg, t);

        applyListeners(msg);

        if (t != null)
            applyListeners(X.getFullStackTrace(t));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable t) {
        if (echo != null)
            echo.error(msg, t);

        applyListeners(msg);

        if (t != null)
            applyListeners(X.getFullStackTrace(t));
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return dbg;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return dbg;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String fileName() {
        return null;
    }

    /**
     * Applies listeners whose pattern is found in the message.
     *
     * @param msg Message to check.
     */
    private void applyListeners(String msg) {
        if (msg == null)
            return;

        for (IgniteInClosure<String> lsnr : lsnrs)
            lsnr.apply(msg);
    }

    public interface LogListenerChainBuilder {
        public LogListenerChainBuilder andContains(String substr);

        public LogListenerChainBuilder andMatch(String regexp);

        public LogListenerChainBuilder andFilter(IgnitePredicate<String> pred);

        public LogListenerChainBuilder times(int n);

        public LogListenerChainBuilder orError(String msg);

        public LogListenerChain listen();
    }

    /** */
    private class LogListenerChainBuilderImpl implements LogListenerChainBuilder {
        /** */
        private final CompositeMessageListener lsnr = new CompositeMessageListener();

        /** */
        private Node prev;

        private void addLast(Node node) {
            if (prev != null)
                lsnr.add(prev.listener());

            prev = node;
        }

        /** {@inheritDoc} */
        @Override public LogListenerChainBuilder andContains(String substr) {
            addLast(new Node(substr, msg -> msg.contains(substr)));

            return this;
        }

        /** {@inheritDoc} */
        @Override public LogListenerChainBuilder andMatch(String regexp) {
            // todo compile pattern
            addLast(new Node(regexp, msg -> msg.matches(regexp)));

            return this;
        }

        /** {@inheritDoc} */
        @Override public LogListenerChainBuilder andFilter(IgnitePredicate<String> pred) {
            addLast(new Node(null, pred));

            return this;
        }

        /** {@inheritDoc} */
        @Override public LogListenerChain listen() {
            addLast(null);

            ListeningTestLogger.this.listen(lsnr);

            return lsnr;
        }

        /** {@inheritDoc} */
        @Override public LogListenerChainBuilder times(int n) {
            if (prev != null)
                prev.times = n;

            return this;
        }

        /** {@inheritDoc} */
        @Override public LogListenerChainBuilder orError(String msg) {
            if (prev != null)
                prev.msg = msg;

            return this;
        }

        /** */
        final class Node {
            /** */
            final String subj;

            /** */
            final IgnitePredicate<String> pred;

            /** */
            Integer times;

            /** */
            String msg;

            /** */
            Node(String subj, IgnitePredicate<String> pred) {
                this.subj = subj;
                this.pred = pred;
            }

            /** */
            LogMessageListener listener() {
                return new LogMessageListener(pred, subj, times, msg);
            }
        }
    }




}
