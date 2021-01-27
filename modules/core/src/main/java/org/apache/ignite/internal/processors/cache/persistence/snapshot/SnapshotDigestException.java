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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Thrown when snapshot integrity is corrupted or impossible to check.
 */
public class SnapshotDigestException extends IgniteCheckedException {
    /**
     * @param msg Message.
     */
    public SnapshotDigestException(String msg) {
        super(msg);
    }

    /**
     * @param cause Cause.
     */
    public SnapshotDigestException(Throwable cause) {
        super(cause);
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param writableStackTrace Writable stack trace.
     */
    public SnapshotDigestException(String msg, @Nullable Throwable cause,
        boolean writableStackTrace) {
        super(msg, cause, writableStackTrace);
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     */
    public SnapshotDigestException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
