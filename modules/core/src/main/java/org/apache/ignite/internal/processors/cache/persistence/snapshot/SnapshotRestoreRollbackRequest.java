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
import org.jetbrains.annotations.Nullable;

/**
 * Request to finalize restored cache startup.
 */
public class SnapshotRestoreRollbackRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Request ID. */
    private final UUID reqId;

//    @GridToStringExclude
//    private final List<StoredCacheData> ccfgs;

    private final Throwable failure;

    private final boolean activateCaches;

    /**
     * @param reqId Request ID.
     * @param updateMetaNodeId Node ID from which to update the binary metadata.
     */
    public SnapshotRestoreRollbackRequest(UUID reqId, boolean activateCaches, @Nullable Throwable failure) {
        assert activateCaches || failure != null;

        this.reqId = reqId;
        this.activateCaches = activateCaches;
        this.failure = failure;
    }

    /**
     * @return Request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    public boolean activateCaches() {
        return activateCaches;
    }

    public @Nullable Throwable failure() {
        return failure;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreRollbackRequest.class, this);
    }
}
