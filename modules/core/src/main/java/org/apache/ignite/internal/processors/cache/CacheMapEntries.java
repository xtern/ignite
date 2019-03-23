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
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.OffheapInvokeAllClosure;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.ATOMIC_VER_COMPARATOR;
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
    private final List<CacheMapEntryInfo> infos = new ArrayList<>(8);

    /** */
    private Set<KeyCacheObject> skipped = new HashSet<>();

    /** */
    private boolean sorted = true;

    /** */
    private KeyCacheObject temp;

    /** */
    public CacheMapEntries(AffinityTopologyVersion topVer, int partId, GridCacheContext cctx, boolean preload) {
        this.topVer = topVer;
        this.cctx = cctx;
        this.preload = preload;
        this.part = cctx.topology().localPartition(partId, topVer, true, true);
    }

    /** */
    public void addEntry(KeyCacheObject key, CacheObject val, long expTime, long ttl, GridCacheVersion ver, GridDrType drType) {
        if (temp != null && sorted && temp.hashCode() >= key.hashCode())
            sorted = false;

        infos.add(new CacheMapEntryInfo(this, temp = key, val, expTime, ttl, ver, drType));
    }

    /** */
    public boolean preload() {
        return preload;
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
    public Collection<KeyCacheObject> keys() {
        return F.viewReadOnly(infos, CacheMapEntryInfo::key);
    }

    /** */
    public Collection<CacheMapEntryInfo> values() {
        return infos;
    }

    /** */
    public CacheMapEntryInfo get(int idx) {
        return infos.get(idx);
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
    public OffheapInvokeAllClosure updateClosure() {
        return new UpdateAllClosure(this, cctx);
    }

    /** */
    public void onRemove(KeyCacheObject key) {
        skipped.add(key);
    }

    /** */
    public void onError(KeyCacheObject key, IgniteCheckedException e) {
        skipped.add(key);
    }

    /** */
    public boolean skip(KeyCacheObject key) {
        return skipped.contains(key);
    }

    /** */
    public void lock() {
        List<GridDhtCacheEntry> locked = new ArrayList<>(infos.size());

        while (true) {
            for (CacheMapEntryInfo info : infos) {
                GridDhtCacheEntry entry = (GridDhtCacheEntry)cctx.cache().entryEx(info.key(), topVer);

                locked.add(entry);

                info.cacheEntry(entry);
            }

            boolean retry = false;

            for (int i = 0; i < locked.size(); i++) {
                GridCacheMapEntry entry = locked.get(i);

                if (entry == null)
                    continue;

                // todo ensure free space
                // todo check obsolete

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
            for (CacheMapEntryInfo info : infos) {
                KeyCacheObject key = info.key();
                GridCacheMapEntry entry = info.cacheEntry();

                if (skipped.contains(key))
                    continue;

                if (entry != null && entry.deleted())
                    skipped.add(entry.key());

                try {
                    info.updateCacheEntry();
                } catch (IgniteCheckedException ex) {
                    skipped.add(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (CacheMapEntryInfo info : values()) {
                GridCacheMapEntry entry = info.cacheEntry();

                if (entry != null)
                    entry.unlockEntry();
            }
        }

        // Try evict partitions.
        for (CacheMapEntryInfo info : values()) {
            GridDhtCacheEntry entry = info.cacheEntry();
            if (entry != null)
                entry.onUnlock();
        }

        if (skipped.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (CacheMapEntryInfo info : values()) {
            GridCacheMapEntry entry = info.cacheEntry();

            if (entry != null && !skipped.contains(entry.key()))
                entry.touch();
        }
    }

    /** */
    private static class UpdateAllClosure implements OffheapInvokeAllClosure {
        /** */
        private static final long serialVersionUID = -4782459128689696534L;

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
        @Override public void call(@Nullable Collection<T2<CacheDataRow, CacheSearchRow>> rows) throws IgniteCheckedException {
            List<DataRow> newRows = new ArrayList<>(8);

            int partId = entries.part().id();
            GridCacheContext cctx = entries.context();

            assert rows.size() == entries.size() : "size mismatch, expected=" + entries.size() + ", input=" + rows.size();

            int idx = 0;

            for (T2<CacheDataRow, CacheSearchRow> t2 : rows) {
                CacheDataRow oldRow = t2.get1();
                KeyCacheObject key = t2.get2().key();

                CacheMapEntryInfo newRowInfo = entries.get(idx++);

                try {
                    if (newRowInfo.needUpdate(oldRow)) {
                        CacheObject val = newRowInfo.value();

                        if (val != null) {
                            if (oldRow != null) {
                                // todo batch updates
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
                                // todo why we need this
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
                catch (GridCacheEntryRemovedException e) {
                    entries.onRemove(key);
                }
            }

            if (!newRows.isEmpty()) {
                //cctx.shared().database().ensureFreeSpace(cctx.dataRegion());

                cctx.offheap().dataStore(entries.part()).rowStore().addRows(newRows, cctx.group().statisticsHolderData());

                if (cacheId == CU.UNDEFINED_CACHE_ID) {
                    // Set cacheId before put keys into tree.
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
            return entries.sorted;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(CacheDataRow row) {
            return false;
        }
    }

    /** */
    private static class CacheMapEntryInfo {
        /** */
        private final CacheMapEntries batch;

        /** */
        private KeyCacheObject key;

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
            KeyCacheObject key,
            CacheObject val,
            long expireTime,
            long ttl,
            GridCacheVersion ver,
            GridDrType drType
        ) {
            this.batch = batch;
            this.key = key;
            this.val = val;
            this.expireTime = expireTime;
            this.ver = ver;
            this.drType = drType;
            this.ttl = ttl;
        }

        /**
         * @return Key.
         */
        public KeyCacheObject key() {
            return key;
        }

        /**
         * @return Value.
         */
        public CacheObject value() {
            return val;
        }

        /**
         * @return Version.
         */
        public GridCacheVersion version() {
            return ver;
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
                        update0 = ATOMIC_VER_COMPARATOR.compare(currVer, version()) < 0;
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
