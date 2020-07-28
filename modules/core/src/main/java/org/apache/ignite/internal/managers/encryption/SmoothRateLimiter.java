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

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SmoothRateLimiter {
    /** Start tick. */
    private final long startTick = System.nanoTime();

    /** Mutex. */
    private final Object mux = new Object();

    /** Warmup period in microsecods. */
    private final long warmupPeriodMicros;

    /**
     * The slope of the line from the stable interval (when permits == 0), to the cold interval
     * (when permits == maxPermits)
     */
    private double slope;

    private double thresholdPermits;

    private double coldFactor;

    /** The currently stored permits. */
    private double storedPermits;

    /** The maximum number of stored permits. */
    private double maxPermits;

    /**
     * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
     * per second has a stable interval of 200ms.
     */
    private double stableIntervalMicros;

    /**
     * The time when the next request (no matter its size) will be granted. After granting a request,
     * this is pushed further in the future. Large requests push this further than small requests.
     */
    private long nextFreeTicketMicros = 0L; // could be either in the past or future

    /*
     * We always hold the mutex when calling this. TODO(cpovirk): Is that important? Perhaps we need
     * to guarantee that each call to reserveEarliestAvailable, etc. sees a value >= the previous?
     * Also, is it OK that we don't hold the mutex when sleeping?
     */
    protected long readMicros() {
        return MICROSECONDS.convert(System.nanoTime() - startTick, NANOSECONDS);
    }

    public SmoothRateLimiter(double permitsPerSecond, long warmupPeriod, TimeUnit timeUnit, double coldFactor) {
        this.coldFactor = coldFactor;
        this.warmupPeriodMicros = timeUnit.toMicros(warmupPeriod);

        setRate(permitsPerSecond);
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
    public void setRate(double permitsPerSecond) {
        A.ensure(permitsPerSecond > 0.0 && !Double.isNaN(permitsPerSecond), "rate must be positive");

        synchronized (mux) {
            doSetRate(permitsPerSecond, readMicros());
        }
    }

    /**
     * Returns the stable rate (as {@code permits per seconds}) with which this {@code RateLimiter} is
     * configured with. The initial value of this is the same as the {@code permitsPerSecond} argument
     * passed in the factory method that produced this {@code RateLimiter}, and it is only updated
     * after invocations to {@linkplain #setRate}.
     */
    public double getRate() {
        synchronized (mux) {
            return SECONDS.toMicros(1L) / stableIntervalMicros;
        }
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
    public double acquire(int permits) throws IgniteInterruptedCheckedException {
        long microsToWait = reserve(permits);

        try {
            MICROSECONDS.sleep(microsToWait);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }

        return 1.0 * microsToWait / SECONDS.toMicros(1L);
    }

    /**
     * Reserves the given number of permits from this {@code RateLimiter} for future use, returning
     * the number of microseconds until the reservation can be consumed.
     *
     * @return time in microseconds to wait until the resource can be acquired, never negative
     */
    final long reserve(int permits) {
        A.ensure(permits > 0, "Requested permits (" + permits + ") must be positive");

        synchronized (mux) {
            long nowMicros = readMicros();

            long momentAvailable = reserveEarliestAvailable(permits, nowMicros);

            return max(momentAvailable - nowMicros, 0);
        }
    }

    final void doSetRate(double permitsPerSecond, long nowMicros) {
        resync(nowMicros);

        double stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;

        this.stableIntervalMicros = stableIntervalMicros;

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

    private long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
        resync(nowMicros);

        long res = nextFreeTicketMicros;

        double storedPermitsToSpend = min(requiredPermits, this.storedPermits);

        double freshPermits = requiredPermits - storedPermitsToSpend;

        long waitMicros =
            storedPermitsToWaitTime(this.storedPermits, storedPermitsToSpend)
                + (long) (freshPermits * stableIntervalMicros);

        this.nextFreeTicketMicros = saturatedAdd(nextFreeTicketMicros, waitMicros);
        this.storedPermits -= storedPermitsToSpend;

        return res;
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
    private long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
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

    /**
     * Returns the number of microseconds during cool down that we have to wait to get a new permit.
     */
    private double coolDownIntervalMicros() {
        return warmupPeriodMicros / maxPermits;
    }

    /** Updates {@code storedPermits} and {@code nextFreeTicketMicros} based on the current time. */
    private void resync(long nowMicros) {
        // if nextFreeTicket is in the past, resync to now
        if (nowMicros > nextFreeTicketMicros) {
            double newPermits = (nowMicros - nextFreeTicketMicros) / coolDownIntervalMicros();

            storedPermits = min(maxPermits, storedPermits + newPermits);

            nextFreeTicketMicros = nowMicros;
        }
    }

    @Override public String toString() {
        return String.format(Locale.ROOT, "RateLimiter[stableRate=%3.1fqps]", getRate());
    }

    public static void main(String[] args) throws IgniteInterruptedCheckedException {
        SmoothRateLimiter limiter = new SmoothRateLimiter(2, 1, TimeUnit.SECONDS, 3.0);

        for (int i = 0; i < 100; i++) {
            limiter.acquire(1);

            System.out.println(">>> " + i);

            if (i == 50) {
                limiter.setRate(5);

                U.sleep(2_000);
            }

        }
    }
}
