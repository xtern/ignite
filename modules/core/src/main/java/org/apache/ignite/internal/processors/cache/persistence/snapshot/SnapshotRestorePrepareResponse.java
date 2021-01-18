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
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.StoredCacheData;

/**
 * Snapshot restore operation single node validation response.
 */
public class SnapshotRestorePrepareResponse implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private List<CacheGroupSnapshotDetails> groups;

    /** */
    public SnapshotRestorePrepareResponse() {
        // No-op.
    }

    /**
     * @param groups List of cache groups snapshot details.
     */
    public SnapshotRestorePrepareResponse(List<CacheGroupSnapshotDetails> groups) {
        this.groups = groups;
    }

    /** todo */
    public List<CacheGroupSnapshotDetails> groups() {
        return groups;
    }

    /** */
    static class CacheGroupSnapshotDetails implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Cache group name. */
        private String grpName;

        /** Local partition IDs. */
        private Set<Integer> parts;

        /** Group cache configurations. */
        private List<StoredCacheData> cfgs;

        private boolean shared;

        /**
         * @param cfgs Group cache configurations.
         * @param parts Local partition IDs.
         */
        public CacheGroupSnapshotDetails(String grpName, boolean shared, List<StoredCacheData> cfgs, Set<Integer> parts) {
            this.grpName = grpName;
            this.shared = shared;
            this.cfgs = cfgs;
            this.parts = parts;
        }

        /**
         * @return Group cache configurations.
         */
        public List<StoredCacheData> configs() {
            return cfgs;
        }

        /**
         * @return Local partition IDs.
         */
        public Set<Integer> parts() {
            return parts;
        }

        /**
         * @return Cache group name.
         */
        public String groupName() {
            return grpName;
        }

        public boolean shared() {
            return shared;
        }
    }
}
