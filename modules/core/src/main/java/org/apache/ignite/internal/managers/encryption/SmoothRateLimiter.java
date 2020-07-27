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

package org.apache.ignite.internal.managers.encryption;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

abstract class SmoothRateLimiter {
    /**
     * The underlying timer; used both to measure elapsed time and sleep as necessary. A separate
     * object to facilitate testing.
     */
    private final SleepingStopwatch stopwatch;
    // Can't be initialized in the constructor because mocks don't call the constructor.
    private volatile Object mutexDoNotUseDirectly;

    public static SmoothRateLimiter create(double permitsPerSecond, long warmupPeriod, TimeUnit unit) {
        checkArgument(warmupPeriod >= 0, "warmupPeriod must not be negative: %s", warmupPeriod);
        return create(
            permitsPerSecond, warmupPeriod, unit, 3.0, new SleepingStopwatch());
    }

    public static void checkArgument(boolean b, @Nullable String errorMessageTemplate, Object ... objs) {
        if (!b)
            throw new IllegalArgumentException(String.format(errorMessageTemplate, objs));
    }

    static SmoothRateLimiter create(
        double permitsPerSecond,
        long warmupPeriod,
        TimeUnit unit,
        double coldFactor,
        SleepingStopwatch stopwatch) {
        SmoothRateLimiter rateLimiter = new SmoothWarmingUp(stopwatch, warmupPeriod, unit, coldFactor);
        rateLimiter.setRate(permitsPerSecond);
        return rateLimiter;
    }

    private static void sleepUninterruptibly(long sleepFor, TimeUnit unit) {
        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(sleepFor);
            long end = System.nanoTime() + remainingNanos;
            while (true) {
                try {
                    // TimeUnit.sleep() treats negative timeouts just like zero.
                    NANOSECONDS.sleep(remainingNanos);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void checkPermits(int permits) {
        if (permits <= 0)
            throw new IllegalStateException("Requested permits (" + permits + ") must be positive");
    }

    private Object mutex() {
        Object mutex = mutexDoNotUseDirectly;
        if (mutex == null) {
            synchronized (this) {
                mutex = mutexDoNotUseDirectly;
                if (mutex == null) {
                    mutexDoNotUseDirectly = mutex = new Object();
                }
            }
        }
        return mutex;
    }

    /**
     * Updates the stable rate of this {@code RateLimiter}, that is, the {@code permitsPerSecond}
     * argument provided in the factory method that constructed the {@code RateLimiter}. Currently
     * throttled threads will <b>not</b> be awakened as a result of this invocation, thus they do not
     * observe the new rate; only subsequent requests will.
     *
     * <p>Note though that, since each request repays (by waiting, if necessary) the cost of the
     * <i>previous</i> request, this means that the very next request after an invocation to {@code
     * setRate} will not be affected by the new rate; it will pay the cost of the previous request,
     * which is in terms of the previous rate.
     *
     * <p>The behavior of the {@code RateLimiter} is not modified in any other way, e.g. if the {@code
     * RateLimiter} was configured with a warmup period of 20 seconds, it still has a warmup period of
     * 20 seconds after this method invocation.
     *
     * @param permitsPerSecond the new stable rate of this {@code RateLimiter}
     * @throws IllegalArgumentException if {@code permitsPerSecond} is negative or zero
     */
    public final void setRate(double permitsPerSecond) {
        checkArgument(
            permitsPerSecond > 0.0 && !Double.isNaN(permitsPerSecond), "rate must be positive");
        synchronized (mutex()) {
            doSetRate(permitsPerSecond, stopwatch.readMicros());
        }
    }

    /**
     * Returns the stable rate (as {@code permits per seconds}) with which this {@code RateLimiter} is
     * configured with. The initial value of this is the same as the {@code permitsPerSecond} argument
     * passed in the factory method that produced this {@code RateLimiter}, and it is only updated
     * after invocations to {@linkplain #setRate}.
     */
    public final double getRate() {
        synchronized (mutex()) {
            return doGetRate();
        }
    }

    /**
     * Acquires a single permit from this {@code RateLimiter}, blocking until the request can be
     * granted. Tells the amount of time slept, if any.
     *
     * <p>This method is equivalent to {@code acquire(1)}.
     *
     * @return time spent sleeping to enforce rate, in seconds; 0.0 if not rate-limited
     * @since 16.0 (present in 13.0 with {@code void} return type})
     */
    public double acquire() {
        return acquire(1);
    }

    /**
     * Acquires the given number of permits from this {@code RateLimiter}, blocking until the request
     * can be granted. Tells the amount of time slept, if any.
     *
     * @param permits the number of permits to acquire
     * @return time spent sleeping to enforce rate, in seconds; 0.0 if not rate-limited
     * @throws IllegalArgumentException if the requested number of permits is negative or zero
     * @since 16.0 (present in 13.0 with {@code void} return type})
     */
    public double acquire(int permits) {
        long microsToWait = reserve(permits);
        stopwatch.sleepMicrosUninterruptibly(microsToWait);
        return 1.0 * microsToWait / SECONDS.toMicros(1L);
    }

    /**
     * Reserves the given number of permits from this {@code RateLimiter} for future use, returning
     * the number of microseconds until the reservation can be consumed.
     *
     * @return time in microseconds to wait until the resource can be acquired, never negative
     */
    final long reserve(int permits) {
        checkPermits(permits);
        synchronized (mutex()) {
            return reserveAndGetWaitLength(permits, stopwatch.readMicros());
        }
    }

    /**
     * Acquires a permit from this {@code RateLimiter} if it can be obtained without exceeding the
     * specified {@code timeout}, or returns {@code false} immediately (without waiting) if the permit
     * would not have been granted before the timeout expired.
     *
     * <p>This method is equivalent to {@code tryAcquire(1, timeout, unit)}.
     *
     * @param timeout the maximum time to wait for the permit. Negative values are treated as zero.
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the permit was acquired, {@code false} otherwise
     * @throws IllegalArgumentException if the requested number of permits is negative or zero
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    /**
     * Acquires permits from this {@link SmoothRateLimiter} if it can be acquired immediately without delay.
     *
     * <p>This method is equivalent to {@code tryAcquire(permits, 0, anyUnit)}.
     *
     * @param permits the number of permits to acquire
     * @return {@code true} if the permits were acquired, {@code false} otherwise
     * @throws IllegalArgumentException if the requested number of permits is negative or zero
     * @since 14.0
     */
    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0, MICROSECONDS);
    }

    /**
     * Acquires a permit from this {@link SmoothRateLimiter} if it can be acquired immediately without
     * delay.
     *
     * <p>This method is equivalent to {@code tryAcquire(1)}.
     *
     * @return {@code true} if the permit was acquired, {@code false} otherwise
     * @since 14.0
     */
    public boolean tryAcquire() {
        return tryAcquire(1, 0, MICROSECONDS);
    }

    /**
     * Acquires the given number of permits from this {@code RateLimiter} if it can be obtained
     * without exceeding the specified {@code timeout}, or returns {@code false} immediately (without
     * waiting) if the permits would not have been granted before the timeout expired.
     *
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait for the permits. Negative values are treated as zero.
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the permits were acquired, {@code false} otherwise
     * @throws IllegalArgumentException if the requested number of permits is negative or zero
     */
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        long timeoutMicros = max(unit.toMicros(timeout), 0);
        checkPermits(permits);
        long microsToWait;
        synchronized (mutex()) {
            long nowMicros = stopwatch.readMicros();
            if (!canAcquire(nowMicros, timeoutMicros)) {
                return false;
            } else {
                microsToWait = reserveAndGetWaitLength(permits, nowMicros);
            }
        }
        stopwatch.sleepMicrosUninterruptibly(microsToWait);
        return true;
    }

    private boolean canAcquire(long nowMicros, long timeoutMicros) {
        return queryEarliestAvailable(nowMicros) - timeoutMicros <= nowMicros;
    }

    /**
     * Reserves next ticket and returns the wait time that the caller must wait for.
     *
     * @return the required wait time, never negative
     */
    final long reserveAndGetWaitLength(int permits, long nowMicros) {
        long momentAvailable = reserveEarliestAvailable(permits, nowMicros);
        return max(momentAvailable - nowMicros, 0);
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "RateLimiter[stableRate=%3.1fqps]", getRate());
    }

    static final class SmoothWarmingUp extends SmoothRateLimiter {
        private final long warmupPeriodMicros;
        /**
         * The slope of the line from the stable interval (when permits == 0), to the cold interval
         * (when permits == maxPermits)
         */
        private double slope;

        private double thresholdPermits;
        private double coldFactor;

        SmoothWarmingUp(
            SmoothRateLimiter.SleepingStopwatch stopwatch, long warmupPeriod, TimeUnit timeUnit, double coldFactor) {
            super(stopwatch);
            this.warmupPeriodMicros = timeUnit.toMicros(warmupPeriod);
            this.coldFactor = coldFactor;
        }

        @Override
        void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
            double oldMaxPermits = maxPermits;
            double coldIntervalMicros = stableIntervalMicros * coldFactor;
            thresholdPermits = 0.5 * warmupPeriodMicros / stableIntervalMicros;
            maxPermits =
                thresholdPermits + 2.0 * warmupPeriodMicros / (stableIntervalMicros + coldIntervalMicros);
            slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits - thresholdPermits);
            if (oldMaxPermits == Double.POSITIVE_INFINITY) {
                // if we don't special-case this, we would get storedPermits == NaN, below
                storedPermits = 0.0;
            } else {
                storedPermits =
                    (oldMaxPermits == 0.0)
                        ? maxPermits // initial state is cold
                        : storedPermits * maxPermits / oldMaxPermits;
            }
        }

        @Override
        long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
            double availablePermitsAboveThreshold = storedPermits - thresholdPermits;
            long micros = 0;
            // measuring the integral on the right part of the function (the climbing line)
            if (availablePermitsAboveThreshold > 0.0) {
                double permitsAboveThresholdToTake = min(availablePermitsAboveThreshold, permitsToTake);
                // TODO(cpovirk): Figure out a good name for this variable.
                double length =
                    permitsToTime(availablePermitsAboveThreshold)
                        + permitsToTime(availablePermitsAboveThreshold - permitsAboveThresholdToTake);
                micros = (long) (permitsAboveThresholdToTake * length / 2.0);
                permitsToTake -= permitsAboveThresholdToTake;
            }
            // measuring the integral on the left part of the function (the horizontal line)
            micros += (long) (stableIntervalMicros * permitsToTake);
            return micros;
        }

        private double permitsToTime(double permits) {
            return stableIntervalMicros + permits * slope;
        }

        @Override
        double coolDownIntervalMicros() {
            return warmupPeriodMicros / maxPermits;
        }
    }

    /** The currently stored permits. */
    double storedPermits;

    /** The maximum number of stored permits. */
    double maxPermits;

    /**
     * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
     * per second has a stable interval of 200ms.
     */
    double stableIntervalMicros;

    /**
     * The time when the next request (no matter its size) will be granted. After granting a request,
     * this is pushed further in the future. Large requests push this further than small requests.
     */
    private long nextFreeTicketMicros = 0L; // could be either in the past or future

    private SmoothRateLimiter(SleepingStopwatch stopwatch) {
        assert stopwatch != null;

        this.stopwatch = stopwatch;
    }

    final void doSetRate(double permitsPerSecond, long nowMicros) {
        resync(nowMicros);
        double stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
        this.stableIntervalMicros = stableIntervalMicros;
        doSetRate(permitsPerSecond, stableIntervalMicros);
    }

    abstract void doSetRate(double permitsPerSecond, double stableIntervalMicros);

    final double doGetRate() {
        return SECONDS.toMicros(1L) / stableIntervalMicros;
    }

    final long queryEarliestAvailable(long nowMicros) {
        return nextFreeTicketMicros;
    }

    final long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
        resync(nowMicros);
        long returnValue = nextFreeTicketMicros;
        double storedPermitsToSpend = min(requiredPermits, this.storedPermits);
        double freshPermits = requiredPermits - storedPermitsToSpend;
        long waitMicros =
            storedPermitsToWaitTime(this.storedPermits, storedPermitsToSpend)
                + (long) (freshPermits * stableIntervalMicros);

        this.nextFreeTicketMicros = saturatedAdd(nextFreeTicketMicros, waitMicros);
        this.storedPermits -= storedPermitsToSpend;
        return returnValue;
    }

    private static long saturatedAdd(long a, long b) {
        long naiveSum = a + b;
        if ((a ^ b) < 0 | (a ^ naiveSum) >= 0) {
            // If a and b have different signs or a has the same sign as the result then there was no
            // overflow, return.
            return naiveSum;
        }
        // we did over/under flow, if the sign is negative we should return MAX otherwise MIN
        return Long.MAX_VALUE + ((naiveSum >>> (Long.SIZE - 1)) ^ 1);
    }

    /**
     * Translates a specified portion of our currently stored permits which we want to spend/acquire,
     * into a throttling time. Conceptually, this evaluates the integral of the underlying function we
     * use, for the range of [(storedPermits - permitsToTake), storedPermits].
     *
     * <p>This always holds: {@code 0 <= permitsToTake <= storedPermits}
     */
    abstract long storedPermitsToWaitTime(double storedPermits, double permitsToTake);

    /**
     * Returns the number of microseconds during cool down that we have to wait to get a new permit.
     */
    abstract double coolDownIntervalMicros();

    /** Updates {@code storedPermits} and {@code nextFreeTicketMicros} based on the current time. */
    void resync(long nowMicros) {
        // if nextFreeTicket is in the past, resync to now
        if (nowMicros > nextFreeTicketMicros) {
            double newPermits = (nowMicros - nextFreeTicketMicros) / coolDownIntervalMicros();
            storedPermits = min(maxPermits, storedPermits + newPermits);
            nextFreeTicketMicros = nowMicros;
        }
    }

    private static class SleepingStopwatch {
        final Stopwatch stopwatch = Stopwatch.createStarted();

        /*
         * We always hold the mutex when calling this. TODO(cpovirk): Is that important? Perhaps we need
         * to guarantee that each call to reserveEarliestAvailable, etc. sees a value >= the previous?
         * Also, is it OK that we don't hold the mutex when sleeping?
         */
        protected long readMicros() {
            return stopwatch.elapsed(MICROSECONDS);
        }

        protected void sleepMicrosUninterruptibly(long micros) {
            if (micros > 0)
                sleepUninterruptibly(micros, MICROSECONDS);
        }
    }

    static final class Stopwatch {
        private boolean isRunning;
        private long elapsedNanos;
        private long startTick;

        /**
         * Creates (and starts) a new stopwatch using {@link System#nanoTime} as its time source.
         *
         * @since 15.0
         */
        public static Stopwatch createStarted() {
            return new Stopwatch().start();
        }

        /**
         * Starts the stopwatch.
         *
         * @return this {@code Stopwatch} instance
         * @throws IllegalStateException if the stopwatch is already running.
         */
        public Stopwatch start() {
            assert !isRunning : "This stopwatch is already running.";

            isRunning = true;
            startTick = System.nanoTime();
            return this;
        }

        /**
         * Stops the stopwatch. Future reads will return the fixed duration that had elapsed up to this
         * point.
         *
         * @return this {@code Stopwatch} instance
         * @throws IllegalStateException if the stopwatch is already stopped.
         */
        public Stopwatch stop() {
            assert isRunning : "This stopwatch is already stopped.";

            long tick = System.nanoTime();
            isRunning = false;
            elapsedNanos += tick - startTick;
            return this;
        }

        private long elapsedNanos() {
            return isRunning ? System.nanoTime() - startTick + elapsedNanos : elapsedNanos;
        }

        /**
         * Returns the current elapsed time shown on this stopwatch, expressed in the desired time unit,
         * with any fraction rounded down.
         *
         * <p><b>Note:</b> the overhead of measurement can be more than a microsecond, so it is generally
         * not useful to specify {@link TimeUnit#NANOSECONDS} precision here.
         *
         * <p>It is generally not a good idea to use an ambiguous, unitless {@code long} to represent
         * elapsed time. Therefore, we recommend using {@link #elapsed()} instead, which returns a
         * strongly-typed {@link Duration} instance.
         *
         * @since 14.0 (since 10.0 as {@code elapsedTime()})
         */
        public long elapsed(TimeUnit desiredUnit) {
            return desiredUnit.convert(elapsedNanos(), NANOSECONDS);
        }

        /**
         * Returns the current elapsed time shown on this stopwatch as a {@link Duration}. Unlike {@link
         * #elapsed(TimeUnit)}, this method does not lose any precision due to rounding.
         *
         * @since 22.0
         */
        public Duration elapsed() {
            return Duration.ofNanos(elapsedNanos());
        }
    }
}
