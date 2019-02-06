package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

public class BatchCacheEntries {
    /** */
    private final int partId;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final Map<KeyCacheObject, BatchedCacheMapEntry> entries = new LinkedHashMap<>();

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private List<GridDhtCacheEntry> cacheEntries;

    /** */
    public BatchCacheEntries(AffinityTopologyVersion topVer, int partId, GridCacheContext cctx) {
        this.topVer = topVer;
        this.cctx = cctx;
        this.partId = partId;
    }

    /** */
    public void addEntry(KeyCacheObject key, CacheObject val, long expTime, GridCacheVersion ver) {
        // todo remove `key` duplication
        entries.put(key, new BatchedCacheMapEntry(key, val, expTime, ver));
    }

    /** */
    public Set<KeyCacheObject> keys() {
        return entries.keySet();
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
    public BatchedCacheMapEntry get(KeyCacheObject key) {
        return entries.get(key);
    }

    public static class BatchedCacheMapEntry {
        private final KeyCacheObject key;
        private final CacheObject val;
        private final long expTime;
        private final GridCacheVersion ver;

        public BatchedCacheMapEntry(KeyCacheObject key, CacheObject val, long expTime,
            GridCacheVersion ver) {
            this.key = key;
            this.val = val;
            this.expTime = expTime;
            this.ver = ver;
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
    }

    public List<GridDhtCacheEntry> lock() {
        cacheEntries = lockEntries(entries.keySet(), topVer);

        return cacheEntries;
    }

    public void unlock() {
        unlockEntries(cacheEntries, topVer);
    }

    public int size() {
        return entries.size();
    }



    private List<GridDhtCacheEntry> lockEntries(Collection<KeyCacheObject> list, AffinityTopologyVersion topVer)
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
            for (KeyCacheObject key : list) {
                GridDhtCacheEntry entry = entryExx(key, topVer);

                locked.add(entry);
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
                return locked;
        }
//        }
    }

    /**
     * Releases java-level locks on cache entries.
     *
     * @param locked Locked entries.
     * @param topVer Topology version.
     */
    private void unlockEntries(List<GridDhtCacheEntry> locked, AffinityTopologyVersion topVer) {
        // Process deleted entries before locks release.
        assert cctx.deferredDelete() : this;

        // Entries to skip eviction manager notification for.
        // Enqueue entries while holding locks.
        Collection<KeyCacheObject> skip = null;

        int size = locked.size();

        try {
            for (int i = 0; i < size; i++) {
                GridCacheMapEntry entry = locked.get(i);
                if (entry != null && entry.deleted()) {
                    if (skip == null)
                        skip = U.newHashSet(locked.size());

                    skip.add(entry.key());
                }
            }
        }
        finally {
            // At least RuntimeException can be thrown by the code above when GridCacheContext is cleaned and there is
            // an attempt to use cleaned resources.
            // That's why releasing locks in the finally block..
            for (int i = 0; i < size; i++) {
                GridCacheMapEntry entry = locked.get(i);
                if (entry != null)
                    entry.unlockEntry();
            }
        }

        // Try evict partitions.
        for (int i = 0; i < size; i++) {
            GridDhtCacheEntry entry = locked.get(i);
            if (entry != null)
                entry.onUnlock();
        }

        if (skip != null && skip.size() == size)
            // Optimization.
            return;

        // Must touch all entries since update may have deleted entries.
        // Eviction manager will remove empty entries.
        for (int i = 0; i < size; i++) {
            GridCacheMapEntry entry = locked.get(i);
            if (entry != null && (skip == null || !skip.contains(entry.key())))
                entry.touch(topVer);
        }
    }

    public GridDhtCacheEntry entryExx(KeyCacheObject key,
        AffinityTopologyVersion topVer) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry)cctx.cache().entryEx(key, topVer);
    }
}
