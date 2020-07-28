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

package org.apache.ignite.internal.util;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class BasicRateLimiterTest {
    /**
     * Check change speed in runtime.
     */
    @Test
    public void checkSpeedLimitChange() throws IgniteInterruptedCheckedException {
        BasicRateLimiter limiter = new BasicRateLimiter(2);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 50; i++) {
            limiter.acquire(1);

            if (i == 10) {
                long passedTime = System.currentTimeMillis() - startTime;

                assertEquals(5, SECONDS.convert(passedTime, MILLISECONDS));

                limiter.setRate(8);
            }
        }

        long passedTime = System.currentTimeMillis() - startTime;

        assertEquals(10, SECONDS.convert(passedTime, MILLISECONDS));
    }
}
