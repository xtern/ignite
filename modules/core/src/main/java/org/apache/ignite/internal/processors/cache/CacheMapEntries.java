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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;

import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.ATOMIC_VER_COMPARATOR;

/**
 * Batch of cache map entries.
 */
public class CacheMapEntries {
    /** */
    static class BatchContext {
        /** */
        private final GridCacheContext cctx;

        /** */
        private final GridDhtLocalPartition part;

        /** */
        private final boolean preload;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private boolean sorted;

        /** */
        private final Set<KeyCacheObject> skipped = new HashSet<>();

        /** */
        BatchContext(GridCacheContext cctx, int partId, boolean preload, AffinityTopologyVersion topVer) {
            this.cctx = cctx;
            this.preload = preload;
            this.topVer = topVer;

            part = cctx.topology().localPartition(partId, topVer, true, true);
        }

        /** */
        void markRemoved(KeyCacheObject key) {
            skipped.add(key);
        }

        /** */
        boolean preload() {
            return preload;
        }

        /** */
        boolean sorted() {
            return sorted;
        }

        /** */
        AffinityTopologyVersion topVer() {
            return topVer;
        }

        /** */
        GridDhtLocalPartition part() {
            return part;
        }

        /** */
        GridCacheContext context() {
            return cctx;
        }

        /** */
        boolean skip(KeyCacheObject key) {
            return skipped.contains(key);
        }

        /** */
        void sorted(boolean sorted) {
            this.sorted = sorted;
        }
    }

    /** */
    public int initialValues(
        List<CacheMapEntryInfo> infos,
        AffinityTopologyVersion topVer,
        GridCacheContext cctx,
        int partId,
        boolean preload
    ) throws IgniteCheckedException {
        BatchContext ctx = new BatchContext(cctx, partId, preload, topVer);

        Collection<CacheMapEntryInfo> locked = lock(ctx, infos);

        try {
            cctx.offheap().updateAll(ctx.context(), ctx.part(), infos, ctx.sorted());
        } finally {
            unlock(ctx, locked);
        }

        return infos.size() - ctx.skipped.size();
    }

    /** */
    private Collection<CacheMapEntryInfo> lock(BatchContext ctx, Collection<CacheMapEntryInfo> infos) {
        List<GridDhtCacheEntry> locked = new ArrayList<>(infos.size());

        boolean ordered = true;

        while (true) {
            Map<KeyCacheObject, CacheMapEntryInfo> uniqueEntries = new HashMap<>();

            KeyCacheObject lastKey = null;

            for (CacheMapEntryInfo e : infos) {
                KeyCacheObject key = e.key();
                CacheMapEntryInfo old = uniqueEntries.put(key, e);

                assert old == null || ATOMIC_VER_COMPARATOR.compare(old.version(), e.version()) < 0 :
                    "Version order mismatch: prev=" + old.version() + ", current=" + e.version();

                if (ordered && lastKey != null && lastKey.hashCode() >= key.hashCode())
                    ordered = false;

                GridDhtCacheEntry entry = (GridDhtCacheEntry)ctx.cctx.cache().entryEx(key, ctx.topVer());

                locked.add(entry);

                e.init(ctx, entry);

                lastKey = key;
            }

            boolean retry = false;

            for (int i = 0; i < locked.size(); i++) {
                GridCacheMapEntry entry = locked.get(i);

                if (entry == null)
                    continue;

                entry.lockEntry();

                if (entry.obsolete()) {
                    // Unlock all locked.
                    for (int j = 0; j <= i; j++) {
                        if (locked.get(j) != null)
                            locked.get(j).unlockEntry();
                    }

                    // Clear entries.
                    locked.clear();

                    // Retry.
                    retry = true;

                    break;
                }
            }

            if (!retry) {
                ctx.sorted(ordered);

                return uniqueEntries.values();
            }
        }
    }

    /**
     * Releases java-level locks on cache entries.
     */
    private void unlock(BatchContext ctx, Collection<CacheMapEntryInfo> infos) {
        // Process deleted entries before locks release.
        // todo
        assert ctx.cctx.deferredDelete() : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        int size = infos.size();

        try {
            for (CacheMapEntryInfo e : infos) {
                KeyCacheObject key = e.key();
                GridCacheMapEntry entry = e.cacheEntry();

                if (ctx.skip(key))
                    continue;

                if (entry != null && entry.deleted())
                    ctx.markRemoved(entry.key());

                try {
                    e.updateCacheEntry();
                } catch (IgniteCheckedException ex) {
                    ctx.markRemoved(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (CacheMapEntryInfo info : infos) {
                GridCacheMapEntry entry = info.cacheEntry();

                if (entry != null)
                    entry.unlockEntry();
            }
        }

        // Try evict partitions.
        for (CacheMapEntryInfo info : infos) {
            GridDhtCacheEntry entry = info.cacheEntry();

            if (entry != null)
                entry.onUnlock();
        }

        if (ctx.skipped.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (CacheMapEntryInfo info : infos) {
            GridCacheMapEntry entry = info.cacheEntry();

            if (entry != null && !ctx.skip(entry.key()))
                entry.touch();
        }
    }
}
