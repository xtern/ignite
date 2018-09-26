package org.apache.ignite.testframework;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.fail;

public class LogListenerBuilder {
    /** */
    private final CompositeMessageListener lsnr = new CompositeMessageListener();

    /** */
    private Node prev;

    private void addLast(Node node) {
        if (prev != null)
            lsnr.add(prev.listener());

        prev = node;
    }

    public LogListenerBuilder matches(String substr) {
        addLast(new Node(substr, msg -> msg.contains(substr)));

        return this;
    }

    public LogListenerBuilder matches(Pattern regexp) {
        addLast(new Node(regexp.toString(), msg -> regexp.matcher(msg).matches()));

        return this;
    }

    public LogListenerBuilder matches(Predicate<String> pred) {
        addLast(new Node(null, pred));

        return this;
    }

    public LogListener build() {
        return lsnr;
    }

    public LogListenerBuilder times(int n) {
        if (prev != null)
            prev.times = n;

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
        Integer times;

        /** */
        String msg;

        /** */
        Node(String subj, Predicate<String> pred) {
            this.subj = subj;
            this.pred = pred;
        }

        /** */
        LogMessageListener listener() {
            return new LogMessageListener(pred, subj, times, msg);
        }
    }

    /** */
    private static class LogMessageListener implements Supplier<Integer>, LogListener {
        /** */
        private final Predicate<String> pred;

        /** */
        private final AtomicReference<Throwable> err = new AtomicReference<>();

        /** */
        private final AtomicInteger appliedCntr = new AtomicInteger();

        /** */
        private final int exp;

        /** */
        private final String subj;

        /** */
        private final String errMsg;

        /**
         *
         * @param subj Search subject.
         * @param pred Search predicate.
         * @param exp Expected occurrences.
         * @param errMsg Custom error message.
         */
        protected LogMessageListener(
            @NotNull Predicate<String> pred,
            @Nullable String subj,
            @Nullable Integer exp,
            @Nullable String errMsg
        ) {
            this.pred = pred;
            this.subj = subj;
            this.exp = exp == null ? -1 : exp;
            this.errMsg = errMsg;
        }

        /** {@inheritDoc} */
        @Override public void accept(String msg) {
            if (err.get() != null)
                return;

            try {
                boolean applied = pred.test(msg);

                if (applied)
                    appliedCntr.incrementAndGet();
            } catch (Throwable t) {
                if (t instanceof VirtualMachineError)
                    throw t;

                err.compareAndSet(null, t);
            }
        }


        /** {@inheritDoc} */
        @Override public Integer get() {
            errCheck();

            return appliedCntr.get();
        }

        /** {@inheritDoc} */
        @Override public void check() {
            String err = null;

            int hitsCnt = get();

            if (exp != -1 && exp != hitsCnt) {
                err = errMsg != null ? errMsg :
                    "Pattern \"" + subj + "\" found " + hitsCnt + " times, expected " + exp + ".";
            }
            else if (hitsCnt == 0)
                err = errMsg != null ? errMsg : "Pattern \"" + subj + "\" not found.";

            if (err != null)
                fail(err);
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
    private static class CompositeMessageListener implements LogListener {
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

//    public LogListenerBuilder matches(String substr)
//
//    public LogListenerBuilder matches(Pattern regexp);
//
//    public LogListenerBuilder matches(Predicate<String> pred);
//
//    public LogListenerBuilder times(int n);
//
//    public LogListenerBuilder orError(String msg);
//
//    public LogListener build();
}
