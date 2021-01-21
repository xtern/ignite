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

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Request to rollback snapshot restore.
 */
public class SnapshotRestoreRollbackRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** The reason to rollback operation. */
    private final Throwable reason;

    /** Request ID. */
    private final UUID reqId;

    /**
     * @param reqId Request ID.
     * @param reason The reason to rollback operation.
     */
    public SnapshotRestoreRollbackRequest(UUID reqId, Throwable reason) {
        this.reqId = reqId;
        this.reason = reason;
    }

    /**
     * @return Request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return The reason to rollback operation.
     */
    public Throwable reason() {
        return reason;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreRollbackRequest.class, this);
    }
}
