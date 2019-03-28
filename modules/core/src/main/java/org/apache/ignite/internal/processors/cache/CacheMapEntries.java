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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.OffheapInvokeAllClosure;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.util.IgniteTree.OperationType.NOOP;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.PUT;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.REMOVE;

/**
 * Batch of cache map entries.
 */
public class CacheMapEntries {
    /** */
    private final GridDhtLocalPartition part;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final boolean preload;

    /** */
    private final LinkedHashMap<KeyCacheObject, CacheMapEntryInfo> infos = new LinkedHashMap<>();

    /** */
    private Set<KeyCacheObject> skipped = new HashSet<>();

    /** */
    private boolean ordered = true;

    /** */
    private KeyCacheObject lastKey;

    /** */
    public CacheMapEntries(AffinityTopologyVersion topVer, int partId, GridCacheContext cctx, boolean preload) {
        this.topVer = topVer;
        this.cctx = cctx;
        this.preload = preload;
        this.part = cctx.topology().localPartition(partId, topVer, true, true);
    }

    /** */
    public void add(KeyCacheObject key, CacheObject val, long expTime, long ttl, GridCacheVersion ver, GridDrType drType) {
        if (lastKey != null && ordered && lastKey.hashCode() >= key.hashCode())
            ordered = false;

        CacheMapEntryInfo old =
            infos.put(lastKey = key, new CacheMapEntryInfo(this, val, expTime, ttl, ver, drType));

        assert old == null || GridCacheMapEntry.ATOMIC_VER_COMPARATOR.compare(old.version(), ver) < 0 :
            "Version order mismatch: prev=" + old.version() + ", current=" + ver;
    }

    /** */
    public GridDhtLocalPartition part() {
        return part;
    }

    /** */
    public GridCacheContext context() {
        return cctx;
    }

    /** */
    public Set<KeyCacheObject> keys() {
        return infos.keySet();
    }

    /**
     * @return Count of batch entries.
     */
    public int size() {
        return infos.size() - skipped.size();
    }

    /**
     * @return Off heap update closure.
     */
    public OffheapInvokeAllClosure offheapUpdateClosure() {
        return new UpdateAllClosure(this, cctx);
    }

    /** */
    public void lock() {
        List<GridDhtCacheEntry> locked = new ArrayList<>(infos.size());

        while (true) {
            for (Map.Entry<KeyCacheObject, CacheMapEntryInfo> e : infos.entrySet()) {
                GridDhtCacheEntry entry = (GridDhtCacheEntry)cctx.cache().entryEx(e.getKey(), topVer);

                locked.add(entry);

                e.getValue().cacheEntry(entry);
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
                return;
        }
    }

    /**
     * Releases java-level locks on cache entries.
     */
    public void unlock() {
        // Process deleted entries before locks release.
        // todo
        assert cctx.deferredDelete() : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        int size = infos.size();

        try {
            for (Map.Entry<KeyCacheObject, CacheMapEntryInfo> e : infos.entrySet()) {
                KeyCacheObject key = e.getKey();
                GridCacheMapEntry entry = e.getValue().cacheEntry();

                if (skipped.contains(key))
                    continue;

                if (entry != null && entry.deleted())
                    skipped.add(entry.key());

                try {
                    e.getValue().updateCacheEntry();
                } catch (IgniteCheckedException ex) {
                    skipped.add(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (CacheMapEntryInfo info : infos.values()) {
                GridCacheMapEntry entry = info.cacheEntry();

                if (entry != null)
                    entry.unlockEntry();
            }
        }

        // Try evict partitions.
        for (CacheMapEntryInfo info : infos.values()) {
            GridDhtCacheEntry entry = info.cacheEntry();

            if (entry != null)
                entry.onUnlock();
        }

        if (skipped.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (CacheMapEntryInfo info : infos.values()) {
            GridCacheMapEntry entry = info.cacheEntry();

            if (entry != null && !skipped.contains(entry.key()))
                entry.touch();
        }
    }

    /** */
    public void onRemove(KeyCacheObject key) {
        skipped.add(key);
    }

    /** */
    boolean preload() {
        return preload;
    }

    /** */
    boolean skip(KeyCacheObject key) {
        return skipped.contains(key);
    }

    /** */
    private static class UpdateAllClosure implements OffheapInvokeAllClosure {
        /** */
        private final List<T3<IgniteTree.OperationType, CacheDataRow, CacheDataRow>> finalOps;

        /** */
        private final int cacheId;

        /** */
        private final CacheMapEntries entries;

        /** */
        public UpdateAllClosure(CacheMapEntries entries, GridCacheContext cctx) {
            this.entries = entries;
            finalOps = new ArrayList<>(entries.size());
            cacheId = cctx.group().storeCacheIdInDataPage() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;
        }

        /** {@inheritDoc} */
        @Override public void call(Collection<CacheDataRow> rows) throws IgniteCheckedException {
            assert rows.size() == entries.size() : "size mismatch, expect=" + entries.size() + ", input=" + rows.size();

            List<DataRow> newRows = new ArrayList<>(8);

            int partId = entries.part().id();
            GridCacheContext cctx = entries.context();

            Iterator<CacheDataRow> rowsIter = rows.iterator();

            for (Map.Entry<KeyCacheObject, CacheMapEntryInfo> e : entries.infos.entrySet()) {
                KeyCacheObject key = e.getKey();
                CacheMapEntryInfo newRowInfo = e.getValue();
                CacheDataRow oldRow = rowsIter.next();

                try {
                    if (newRowInfo.needUpdate(oldRow)) {
                        CacheObject val = newRowInfo.value();

                        if (val != null) {
                            if (oldRow != null) {
                                CacheDataRow newRow = cctx.offheap().dataStore(entries.part()).createRow(
                                    cctx,
                                    key,
                                    newRowInfo.value(),
                                    newRowInfo.version(),
                                    newRowInfo.expireTime(),
                                    oldRow);

                                finalOps.add(new T3<>(oldRow.link() == newRow.link() ? NOOP : PUT, oldRow, newRow));
                            }
                            else {
                                CacheObjectContext coCtx = cctx.cacheObjectContext();

                                val.valueBytes(coCtx);
                                key.valueBytes(coCtx);

                                if (key.partition() == -1)
                                    key.partition(partId);

                                DataRow newRow = new DataRow(key, val, newRowInfo.version(), partId,
                                    newRowInfo.expireTime(), cacheId);

                                newRows.add(newRow);

                                finalOps.add(new T3<>(PUT, oldRow, newRow));
                            }
                        }
                        else {
                            // todo   we should pass key somehow to remove old row
                            // todo   (in particular case oldRow should not contain key)
                            DataRow newRow = new DataRow(key, null, null, 0, 0, cacheId);

                            finalOps.add(new T3<>(oldRow != null ? REMOVE : NOOP, oldRow, newRow));
                        }
                    }
                }
                catch (GridCacheEntryRemovedException ex) {
                    entries.onRemove(key);
                }
            }

            if (!newRows.isEmpty()) {
                // cctx.shared().database().ensureFreeSpace(cctx.dataRegion());

                cctx.offheap().dataStore(entries.part()).rowStore().
                    addRows(newRows, cctx.group().statisticsHolderData());

                if (cacheId == CU.UNDEFINED_CACHE_ID) {
                    // Set cacheId before write keys into tree.
                    for (DataRow row : newRows)
                        row.cacheId(cctx.cacheId());

                }
            }
        }

        /** {@inheritDoc} */
        @Override public Collection<T3<IgniteTree.OperationType, CacheDataRow, CacheDataRow>> result() {
            return finalOps;
        }

        /** {@inheritDoc} */
        @Override public boolean fastpath() {
            return entries.ordered;
        }
    }

    /** */
    private static class CacheMapEntryInfo {
        /** */
        private final CacheMapEntries batch;

        /** */
        private final CacheObject val;

        /** */
        private final long expireTime;

        /** */
        private final long ttl;

        /** */
        private final GridCacheVersion ver;

        /** */
        private final GridDrType drType;

        /** */
        private GridDhtCacheEntry entry;

        /** */
        private boolean update;

        /** */
        public CacheMapEntryInfo(
            CacheMapEntries batch,
            CacheObject val,
            long expireTime,
            long ttl,
            GridCacheVersion ver,
            GridDrType drType
        ) {
            this.batch = batch;
            this.val = val;
            this.expireTime = expireTime;
            this.ver = ver;
            this.drType = drType;
            this.ttl = ttl;
        }

        /**
         * @return Version.
         */
        public GridCacheVersion version() {
            return ver;
        }

        /**
         * @return Value.
         */
        public CacheObject value() {
            return val;
        }

        /**
         * @return Expire time.
         */
        public long expireTime() {
            return expireTime;
        }

        /**
         * @param entry Cache entry.
         */
        public void cacheEntry(GridDhtCacheEntry entry) {
            this.entry = entry;
        }

        /**
         * @return Cache entry.
         */
        public GridDhtCacheEntry cacheEntry() {
            return entry;
        }

        /** */
        public void updateCacheEntry() throws IgniteCheckedException {
            if (!update)
                return;

            entry.finishInitialUpdate(val, expireTime, ttl, ver, batch.topVer, drType, null, batch.preload);
        }

        /** */
        public boolean needUpdate(CacheDataRow row) throws GridCacheEntryRemovedException {
            GridCacheVersion currVer = row != null ? row.version() : entry.version();

            GridCacheContext cctx = batch.context();

            boolean isStartVer = cctx.versions().isStartVersion(currVer);

            boolean update0;

            if (cctx.group().persistenceEnabled()) {
                if (!isStartVer) {
                    if (cctx.atomic())
                        update0 = GridCacheMapEntry.ATOMIC_VER_COMPARATOR.compare(currVer, version()) < 0;
                    else
                        update0 = currVer.compareTo(version()) < 0;
                }
                else
                    update0 = true;
            }
            else
                update0 = (isStartVer && row == null);

            update0 |= (!batch.preload() && entry.deletedUnlocked());

            update = update0;

            return update0;
        }
    }
}
