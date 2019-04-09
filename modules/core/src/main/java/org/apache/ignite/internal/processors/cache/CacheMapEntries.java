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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.ATOMIC_VER_COMPARATOR;

/**
 * Batch of cache map entries.
 */
public class CacheMapEntries {
    /**
     *
     */
    private static class GridCacheEntryInfoEx extends GridCacheEntryInfo {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridCacheEntryInfo delegate;

        /** */
        private GridDhtCacheEntry cacheEntry;

        /** */
        private boolean update;

        /** */
        private GridCacheEntryInfoEx(GridCacheEntryInfo info) {
            delegate = info;
        }

        /**
         * @return Key.
         */
        @Override public KeyCacheObject key() {
            return delegate.key();
        }

        /**
         * @return Entry value.
         */
        @Override public CacheObject value() {
            return delegate.value();
        }

        /**
         * @return Expire time.
         */
        @Override public long expireTime() {
            return delegate.expireTime();
        }

        /**
         * @return Time to live.
         */
        @Override public long ttl() {
            return delegate.ttl();
        }

        /**
         * @return Time to live.
         */
        @Override public GridCacheVersion version() {
            return delegate.version();
        }
    }


    /** */
    private static class BatchContext {
        /** */
        private final GridCacheContext<?, ?> cctx;

        /** */
        private final GridDhtLocalPartition part;

        /** */
        private final boolean preload;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final Set<KeyCacheObject> skipped = new HashSet<>();

        /** */
        private boolean sorted;

        /** */
        BatchContext(GridCacheContext<?, ?> cctx, GridDhtLocalPartition part, boolean preload, AffinityTopologyVersion topVer) {
            this.cctx = cctx;
            this.preload = preload;
            this.topVer = topVer;
            this.part = part;
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
        GridCacheContext<?, ?> context() {
            return cctx;
        }

        /** */
        boolean skipped(KeyCacheObject key) {
            return skipped.contains(key);
        }

        /** */
        void sorted(boolean sorted) {
            this.sorted = sorted;
        }
    }

    /** */
    public int initialValues(
        List<GridCacheEntryInfo> infos,
        AffinityTopologyVersion topVer,
        GridCacheContext cctx,
        int partId,
        boolean preload,
        GridDrType drType
    ) throws IgniteCheckedException {
        GridDhtLocalPartition part = cctx.topology().localPartition(partId, topVer, true, true);

        BatchContext ctx = new BatchContext(cctx, part, preload, topVer);

        Collection<GridCacheEntryInfoEx> locked = initialValuesLock(ctx, infos);

        try {
            IgniteBiPredicate<CacheDataRow, GridCacheEntryInfo> pred  =
                new IgniteBiPredicate<CacheDataRow, GridCacheEntryInfo>() {
                @Override public boolean apply(CacheDataRow row, GridCacheEntryInfo info) {
                    try {
                        GridCacheVersion currVer = row != null ? row.version() :
                            ((GridCacheEntryInfoEx)info).cacheEntry.version();

                        GridCacheContext cctx = ctx.context();

                        boolean isStartVer = cctx.versions().isStartVersion(currVer);

                        boolean update0;

                        if (cctx.group().persistenceEnabled()) {
                            if (!isStartVer) {
                                if (cctx.atomic())
                                    update0 = ATOMIC_VER_COMPARATOR.compare(currVer, info.version()) < 0;
                                else
                                    update0 = currVer.compareTo(info.version()) < 0;
                            }
                            else
                                update0 = true;
                        }
                        else
                            update0 = (isStartVer && row == null);

                        update0 |= (!ctx.preload() && ((GridCacheEntryInfoEx)info).cacheEntry.deletedUnlocked());

                        ((GridCacheEntryInfoEx)info).update = update0;

                        return update0;
                    }
                    catch (GridCacheEntryRemovedException e) {
                        ctx.markRemoved(info.key());

                        return false;
                    }
                }
            };

            cctx.offheap().updateAll(cctx, part, locked, pred);
        } finally {
            initialValuesUnlock(ctx, locked, drType);
        }

        return infos.size() - ctx.skipped.size();
    }

    /** */
    private Collection<GridCacheEntryInfoEx> initialValuesLock(BatchContext ctx, Collection<GridCacheEntryInfo> infos) {
        List<GridDhtCacheEntry> locked = new ArrayList<>(infos.size());

        while (true) {
            Map<KeyCacheObject, GridCacheEntryInfoEx> uniqueEntries = new LinkedHashMap<>();

            for (GridCacheEntryInfo e : infos) {
                KeyCacheObject key = e.key();

                GridCacheEntryInfoEx entryEx = new GridCacheEntryInfoEx(e);

                GridCacheEntryInfoEx old = uniqueEntries.put(key, entryEx);

                assert old == null || ATOMIC_VER_COMPARATOR.compare(old.version(), e.version()) < 0 :
                    "Version order mismatch: prev=" + old.version() + ", current=" + e.version();

                GridDhtCacheEntry entry = (GridDhtCacheEntry)ctx.cctx.cache().entryEx(key, ctx.topVer());

                locked.add(entry);

                entryEx.cacheEntry = entry;
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

            if (!retry)
                return uniqueEntries.values();
        }
    }

    /** */
    private void initialValuesUnlock(BatchContext ctx, Collection<GridCacheEntryInfoEx> infos, GridDrType drType) {
        // Process deleted entries before locks release.
        // todo
        assert ctx.cctx.deferredDelete() : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        int size = infos.size();

        try {
            for (GridCacheEntryInfoEx info : infos) {
                KeyCacheObject key = info.key();
                GridCacheMapEntry entry = info.cacheEntry;

                assert entry != null : key;

                if (!info.update)
                    continue;

                if (ctx.skipped(key))
                    continue;

                if (entry.deleted()) {
                    ctx.markRemoved(key);

                    continue;
                }

                try {
                    entry.finishInitialUpdate(info.value(), info.expireTime(), info.ttl(), info.version(), ctx.topVer(),
                        drType, null, ctx.preload());
                } catch (IgniteCheckedException ex) {
                    ctx.context().logger(getClass()).error("Unable to finish initial update, skip " + key, ex);

                    ctx.markRemoved(key);
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (GridCacheEntryInfoEx info : infos) {
                GridCacheMapEntry entry = info.cacheEntry;

                if (entry != null)
                    entry.unlockEntry();
            }
        }

        // Try evict partitions.
        for (GridCacheEntryInfoEx info : infos) {
            GridDhtCacheEntry entry = info.cacheEntry;

            if (entry != null)
                entry.onUnlock();
        }

        if (ctx.skipped.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (GridCacheEntryInfoEx info : infos) {
            GridCacheMapEntry entry = info.cacheEntry;

            if (entry != null && !ctx.skipped(entry.key()))
                entry.touch();
        }
    }
}
