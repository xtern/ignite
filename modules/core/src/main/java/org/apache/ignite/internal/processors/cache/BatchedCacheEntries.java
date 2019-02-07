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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.ATOMIC_VER_COMPARATOR;

/**
 * Batch of cache entries to optimize page memory processing.
 */
public class BatchedCacheEntries {
    /** */
    private final int partId;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final Map<KeyCacheObject, BatchedCacheMapEntryInfo> infos = new LinkedHashMap<>();

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final boolean preload;

    /** */
    private List<GridDhtCacheEntry> entries;

    /** */
    public BatchedCacheEntries(AffinityTopologyVersion topVer, int partId, GridCacheContext cctx, boolean preload) {
        this.topVer = topVer;
        this.cctx = cctx;
        this.partId = partId;
        this.preload = preload;
    }

    /** */
    public void addEntry(KeyCacheObject key, CacheObject val, long expTime, long ttl, GridCacheVersion ver, GridDrType drType) {
        // todo remove `key` duplication (Map<Key, Entry<Key...)
        infos.put(key, new BatchedCacheMapEntryInfo(this, key, val, expTime, ttl, ver, drType));
    }

    /** */
    public Set<KeyCacheObject> keys() {
        return infos.keySet();
    }

    /** */
    public int part() {
        return partId;
    }

    /** */
    public GridCacheContext context() {
        return cctx;
    }

    /** */
    public BatchedCacheMapEntryInfo get(KeyCacheObject key) {
        return infos.get(key);
    }

    /** */
    public boolean preload() {
        return preload;
    }

    /** */
    public boolean needUpdate(KeyCacheObject key, CacheDataRow row) throws GridCacheEntryRemovedException {
        BatchedCacheMapEntryInfo info = infos.get(key);

        GridCacheVersion currVer = row != null ? row.version() : info.entry.version();

        boolean isStartVer = cctx.shared().versions().isStartVersion(currVer);

        boolean update;

        if (cctx.group().persistenceEnabled()) {
            if (!isStartVer) {
                if (cctx.atomic())
                    update = ATOMIC_VER_COMPARATOR.compare(currVer, info.version()) < 0;
                else
                    update = currVer.compareTo(info.version()) < 0;
            }
            else
                update = true;
        }
        else
            update = isStartVer;

        // todo update0 |= (!preload && deletedUnlocked());

        info.update(update);

        return update;
    }

    public void onRemove(KeyCacheObject key) {
        // todo  - remove from original collection
    }

    public void onError(KeyCacheObject key, IgniteCheckedException e) {
        // todo  - remove from original collection
    }

    public boolean skip(KeyCacheObject key) {
        // todo
        return false;
    }

    public static class BatchedCacheMapEntryInfo {
        // todo think about remove
        private final BatchedCacheEntries batch;
        private final KeyCacheObject key;
        private final CacheObject val;
        private final long expTime;
        private final long ttl;
        private final GridCacheVersion ver;
        private final GridDrType drType;

        private GridDhtCacheEntry entry;

        private boolean update;

        public BatchedCacheMapEntryInfo(
            BatchedCacheEntries batch,
            KeyCacheObject key,
            CacheObject val,
            long expTime,
            long ttl,
            GridCacheVersion ver,
            GridDrType drType
        ) {
            this.batch = batch;
            this.key = key;
            this.val = val;
            this.expTime = expTime;
            this.ver = ver;
            this.drType = drType;
            this.ttl = ttl;
        }

        public KeyCacheObject key() {
            return key;
        }

        public GridCacheVersion version() {
            return ver;
        }

        public CacheObject value() {
            return val;
        }

        public long expireTime() {
            return expTime;
        }

        public GridDhtCacheEntry cacheEntry() {
            return entry;
        }

        public void cacheEntry(GridDhtCacheEntry entry) {
            this.entry = entry;
        }

        public void updateCacheEntry() throws IgniteCheckedException {
            if (!update)
                return;

            entry.finishPreload(val, expTime, ttl, ver, batch.topVer, drType, null, batch.preload);
        }

        public void update(boolean update) {
            this.update = update;
        }
    }

    public List<GridDhtCacheEntry> lock() {
        entries = lockEntries(infos.values(), topVer);

        return entries;
    }

    public void unlock() {
        unlockEntries(infos.values(), topVer);
    }

    public int size() {
        return infos.size();
    }

    private List<GridDhtCacheEntry> lockEntries(Collection<BatchedCacheMapEntryInfo> list, AffinityTopologyVersion topVer)
        throws GridDhtInvalidPartitionException {
//        if (req.size() == 1) {
//            KeyCacheObject key = req.key(0);
//
//            while (true) {
//                GridDhtCacheEntry entry = entryExx(key, topVer);
//
//                entry.lockEntry();
//
//                if (entry.obsolete())
//                    entry.unlockEntry();
//                else
//                    return Collections.singletonList(entry);
//            }
//        }
//        else {
        List<GridDhtCacheEntry> locked = new ArrayList<>(list.size());

        while (true) {
            for (BatchedCacheMapEntryInfo info : list) {
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
                return locked;
        }
//        }
    }

    /**
     * Releases java-level locks on cache entries
     * todo carefully think about possible reorderings in locking/unlocking.
     *
     * @param locked Locked entries.
     * @param topVer Topology version.
     */
    private void unlockEntries(Collection<BatchedCacheMapEntryInfo> locked, AffinityTopologyVersion topVer) {
        // Process deleted entries before locks release.
        assert cctx.deferredDelete() : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        // todo Common skip list.
        Collection<KeyCacheObject> skip = null;

        int size = locked.size();

        try {
            for (BatchedCacheMapEntryInfo info : locked) {
                GridCacheMapEntry entry = info.cacheEntry();

                if (entry != null && entry.deleted()) {
                    if (skip == null)
                        skip = U.newHashSet(locked.size());

                    skip.add(entry.key());
                }

                try {
                    info.updateCacheEntry();
                } catch (IgniteCheckedException e) {
                    skip.add(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (BatchedCacheMapEntryInfo info : locked) {
                GridCacheMapEntry entry = info.cacheEntry();
                if (entry != null)
                    entry.unlockEntry();
            }
        }

        // Try evict partitions.
        for (BatchedCacheMapEntryInfo info : locked) {
            GridDhtCacheEntry entry = info.cacheEntry();
            if (entry != null)
                entry.onUnlock();
        }

        if (skip != null && skip.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (BatchedCacheMapEntryInfo info : locked) {
            GridCacheMapEntry entry = info.cacheEntry();
            if (entry != null && (skip == null || !skip.contains(entry.key())))
                entry.touch(topVer);
        }
    }
}
