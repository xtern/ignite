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
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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
        private GridCacheMapEntry cacheEntry;

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
    public Collection<Map.Entry<GridCacheEntryInfo, GridCacheMapEntry>> initialValues(
        List<GridCacheEntryInfo> infos,
        AffinityTopologyVersion topVer,
        GridCacheContext cctx,
        int partId,
        boolean preload,
        GridDrType drType
    ) throws IgniteCheckedException {
        GridDhtLocalPartition part = cctx.topology().localPartition(partId, topVer, true, true);

        Set<KeyCacheObject> skipped = new HashSet<>();

        Collection<GridCacheEntryInfoEx> locked = lockEntries(cctx, topVer, infos);

        try {
            IgniteBiPredicate<CacheDataRow, GridCacheEntryInfo> pred  =
                new IgniteBiPredicate<CacheDataRow, GridCacheEntryInfo>() {
                @Override public boolean apply(CacheDataRow row, GridCacheEntryInfo info) {
                    try {
                        GridCacheVersion currVer = row != null ? row.version() :
                            ((GridCacheEntryInfoEx)info).cacheEntry.version();

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

                        update0 |= (!preload && ((GridCacheEntryInfoEx)info).cacheEntry.deletedUnlocked());

                        ((GridCacheEntryInfoEx)info).update = update0;

                        return update0;
                    }
                    catch (GridCacheEntryRemovedException e) {
                        skipped.add(info.key());

                        return false;
                    }
                }
            };

            cctx.offheap().updateAll(cctx, part, locked, pred);
        } finally {
            unlockEntries(cctx, topVer, preload, drType, locked, skipped);
        }

        return F.viewReadOnly(locked, v -> new T2<>(v, v.cacheEntry), v -> !skipped.contains(v.key()));
    }

    /** */
    private Collection<GridCacheEntryInfoEx> lockEntries(
        GridCacheContext cctx,
        AffinityTopologyVersion topVer,
        Collection<GridCacheEntryInfo> infos
    ) {
        List<GridCacheMapEntry> locked = new ArrayList<>(infos.size());

        while (true) {
            Map<KeyCacheObject, GridCacheEntryInfoEx> uniqueEntries = new LinkedHashMap<>();

            for (GridCacheEntryInfo e : infos) {
                KeyCacheObject key = e.key();

                GridCacheEntryInfoEx entryEx = new GridCacheEntryInfoEx(e);

                GridCacheEntryInfoEx old = uniqueEntries.put(key, entryEx);

                assert old == null || ATOMIC_VER_COMPARATOR.compare(old.version(), e.version()) < 0 :
                    "Version order mismatch: prev=" + old.version() + ", current=" + e.version();

                GridCacheMapEntry entry = (GridCacheMapEntry)cctx.cache().entryEx(key, topVer);

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
    private void unlockEntries(
        GridCacheContext<?, ?> cctx,
        AffinityTopologyVersion topVer,
        boolean preload,
        GridDrType drType,
        Collection<GridCacheEntryInfoEx> infos,
        Set<KeyCacheObject> skippedKeys
    ) {
        try {
            for (GridCacheEntryInfoEx info : infos) {
                KeyCacheObject key = info.key();
                GridCacheMapEntry entry = info.cacheEntry;

                assert entry != null : key;

                if (!info.update)
                    continue;

                if (skippedKeys.contains(key))
                    continue;

                if (entry.deleted()) {
                    skippedKeys.add(key);

                    continue;
                }

                try {
                    entry.finishInitialUpdate(info.value(), info.expireTime(), info.ttl(), info.version(), topVer,
                        drType, null, preload);
                } catch (IgniteCheckedException ex) {
                    cctx.logger(getClass()).error("Unable to finish initial update, skip " + key, ex);

                    skippedKeys.add(key);
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
            GridCacheMapEntry entry = info.cacheEntry;

            if (entry != null)
                entry.onUnlock();
        }
    }
}
