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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Simplified version of guava's smooth RateLimiter.
 *
 * The primary feature of a BasicRateLimiter is its "stable rate", the maximum rate that is should
 * allow at normal conditions. This is enforced by "throttling" incoming requests as needed, i.e.
 * compute, for an incoming request, the appropriate throttle time, and make the calling thread
 * wait as much.
 *
 * The simplest way to maintain a rate of QPS is to keep the timestamp of the last granted
 * request, and ensure that (1/QPS) seconds have elapsed since then. For example, for a rate of
 * QPS=5 (5 tokens per second), if we ensure that a request isn't granted earlier than 200ms after
 * the last one, then we achieve the intended rate. If a request comes and the last request was
 * granted only 100ms ago, then we wait for another 100ms. At this rate, serving 15 fresh permits
 * (i.e. for an acquire(15) request) naturally takes 3 seconds.
 *
 * It is important to realize that such a RateLimiter has a very superficial memory of the past:
 * it only remembers the last request. if the RateLimiter was unused for a long period of
 * time, then a request arrived and was immediately granted? This RateLimiter would immediately
 * forget about that past underutilization.
 */
public class BasicRateLimiter {
    /** Start timestamp. */
    private final long startTime = System.nanoTime();

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
     * per second has a stable interval of 200ms.
     */
    private double stableIntervalMicros;

    /**
     * The time when the next request (no matter its size) will be granted. After granting a request,
     * this is pushed further in the future. Large requests push this further than small requests.
     */
    private long nextFreeTicketMicros;

    /**
     * @param permitsPerSecond Estimated number of permits per second.
     */
    public BasicRateLimiter(double permitsPerSecond) {
        setRate(permitsPerSecond);
    }

    /**
     * Updates the stable rate.
     *
     * @param permitsPerSecond the new stable rate of this {@code RateLimiter}
     * @throws IllegalArgumentException if {@code permitsPerSecond} is negative or zero
     */
    public void setRate(double permitsPerSecond) {
        A.ensure(permitsPerSecond > 0.0 && !Double.isNaN(permitsPerSecond), "rate must be positive");

        synchronized (mux) {
            resync();

            stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
        }
    }

    /**
     * @return The stable rate (as {@code permits per seconds}).
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
     * Reserves the given number of permits for future use.
     *
     * @param permits The number of permits.
     * @return time in microseconds to wait until the resource can be acquired, never negative
     */
    private long reserve(int permits) {
        A.ensure(permits > 0, "Requested permits (" + permits + ") must be positive");

        synchronized (mux) {
            long nowMicros = resync();

            long momentAvailable = nextFreeTicketMicros;

            nextFreeTicketMicros = saturatedAdd(momentAvailable, (long)(permits * stableIntervalMicros));

            return max(momentAvailable - nowMicros, 0);
        }
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
     * Updates {@code nextFreeTicketMicros} based on the current time.
     *
     * @return passed time since start in microseconds.
     */
    private long resync() {
        long passed = MICROSECONDS.convert(System.nanoTime() - startTime, NANOSECONDS);

        // if nextFreeTicket is in the past, resync to now
        if (passed > nextFreeTicketMicros)
            nextFreeTicketMicros = passed;

        return passed;
    }

    @Override public String toString() {
        return String.format(Locale.ROOT, "RateLimiter[stableRate=%3.1fqps]", getRate());
    }

    public static void main(String[] args) throws IgniteInterruptedCheckedException {
        BasicRateLimiter limiter = new BasicRateLimiter(2);

        long start = System.currentTimeMillis();

        for (int i = 0; i < 50; i++) {
            limiter.acquire(1);

            System.out.println(">>> " + i);

            if (i == 20) {
                System.out.println("total: " + (System.currentTimeMillis() - start) / 1000);

                limiter.setRate(5);

                U.sleep(2_000);

                start = System.currentTimeMillis();
            }
        }

        System.out.println("total: " + (System.currentTimeMillis() - start) / 1000);
    }
}
