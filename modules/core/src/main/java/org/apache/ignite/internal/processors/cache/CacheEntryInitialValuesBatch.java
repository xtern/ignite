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
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.ATOMIC_VER_COMPARATOR;
import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.IS_UNSWAPPED_MASK;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;

/** */
public class CacheEntryInitialValuesBatch {
    /** */
    private final GridCacheContext cctx;

    /** */
    public CacheEntryInitialValuesBatch(GridCacheContext cctx) {
        this.cctx = cctx;
    }

    /** */
    private List<InitialValue> initialValues = new ArrayList<>(1);

    class InitialValue {
        private final GridCacheMapEntry entry;
        private final CacheObject val;
        private final GridCacheVersion ver;
        private final MvccVersion mvccVer;
        private final MvccVersion newMvccVer;
        private final long ttl;
        private final long expireTime;
        private final boolean preload;
        private final AffinityTopologyVersion topVer;
        private final GridDrType drType;
        private final boolean fromStore;

        private Runnable unlockCb;
//        private boolean obsolete;

        public InitialValue(GridCacheMapEntry entry, CacheObject val,
            GridCacheVersion ver, MvccVersion mvccVer, MvccVersion newMvccVer, long ttl, long expireTime,
            boolean preload,
            AffinityTopologyVersion topVer, GridDrType drType, boolean fromStore) {
            this.entry = entry;
            this.val = val;
            this.ver = ver;
            this.mvccVer = mvccVer;
            this.newMvccVer = newMvccVer;
            this.ttl = ttl;
            this.expireTime = expireTime;
            this.preload = preload;
            this.topVer = topVer;
            this.drType = drType;
            this.fromStore = fromStore;
        }
    }

    /** */
    public CacheEntryInitialValuesBatch add(
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        long ttl,
        long expireTime,
        boolean preload,
        AffinityTopologyVersion topVer,
        GridDrType drType,
        boolean fromStore
    ) {
        initialValues.add(new InitialValue(entry,
            val,
            ver,
            mvccVer,
            newMvccVer,
            ttl,
            expireTime,
            preload,
            topVer,
            drType,
            fromStore));

        return this;
    }

    /** */
    public int initValues() throws IgniteCheckedException, GridCacheEntryRemovedException {
        int initCnt = 0;

        cctx.shared().database().ensureFreeSpace(cctx.dataRegion());

        cctx.group().listenerLock().readLock().lock();

        lockEntries();

        try {
            for (InitialValue val : initialValues) {
                 val.entry.lockEntry();

                try {
                    if (initialValue(val))
                        ++initCnt;
                } finally {
                    val.entry.unlockEntry();
                }
            }
        } finally {
            unlockEntries();

            cctx.group().listenerLock().readLock().unlock();
        }

        return initCnt;
    }

    /** */
    private void unlockEntries() {
        for (InitialValue val : initialValues) {
            val.entry.unlockEntry();

            if (val.unlockCb != null)
                val.unlockCb.run();
        }
    }

    /** */
    private void lockEntries() {
        // todo improve by copy-pasting from atomic cache
        for (InitialValue val : initialValues)
            val.entry.lockEntry();
    }

    private boolean initialValue(InitialValue iv) throws IgniteCheckedException, GridCacheEntryRemovedException {
        GridCacheMapEntry entry = iv.entry;
        CacheObject val = iv.val;
        GridCacheVersion ver = iv.ver;
        MvccVersion mvccVer = iv.mvccVer;
        MvccVersion newMvccVer = iv.newMvccVer;
        long ttl = iv.ttl;
        long expireTime = iv.expireTime;
        boolean preload = iv.preload;
        AffinityTopologyVersion topVer = iv.topVer;
        GridDrType drType = iv.drType;
        boolean fromStore = iv.fromStore;

//        entry.ensureFreeSpace();

//        boolean deferred = false;
//        boolean obsolete = false;

//        GridCacheVersion oldVer = null;

//        entry.lockListenerReadLock();
//        entry.lockEntry();

//        try {
            entry.checkObsolete();

            long expTime = expireTime < 0 ? CU.toExpireTime(ttl) : expireTime;

            val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

            final boolean unswapped = ((entry.flags & IS_UNSWAPPED_MASK) != 0);

            boolean update = false;

            IgnitePredicate<CacheDataRow> p = new InitialValuePredicate(entry, ver, preload);

            if (unswapped || cctx.mvccEnabled()) {
                if (!unswapped)
                    entry.unswap(false);

                if (update = p.apply(null)) {
                    // If entry is already unswapped and we are modifying it, we must run deletion callbacks for old value.
                    long oldExpTime = entry.expireTimeUnlocked();

                    if (oldExpTime > 0 && oldExpTime < U.currentTimeMillis()) {
                        if (entry.onExpired(entry.val, null)) {
                            if (cctx.deferredDelete()) {
                                final GridCacheVersion oldVer = entry.ver;

                                iv.unlockCb = () -> cctx.onDeferredDelete(entry, oldVer);
                            }
                            else if (val == null) {
                                iv.unlockCb = () -> {
                                    entry.onMarkedObsolete();

                                    cctx.cache().removeEntry(entry);
                                };
                            }
                        }
                    }

//                    if (!cctx.mvccEnabled())
                    p = null;
                }
            }

            if (cctx.mvccEnabled()) {
                assert !preload;

                cctx.offheap().mvccInitialValue(entry, val, ver, expTime, mvccVer, newMvccVer);
            }
            else { // !cctx.mvccEnabled() && (!unswapped  || (unswapped && p == null))
                boolean update0 = entry.storeValue(val, expTime, ver, p);

                if (p != null)
                    update = update0;
            }

            if (update) {
                finishInitialUpdate(entry, val, expireTime, ttl, ver, topVer, drType, mvccVer, preload, fromStore);

                return true;
            }

            return false;
//        }
//        finally {
//            entry.unlockEntry();
//            entry.unlockListenerReadLock();


//        }
    }

    /**
     * todo explain this and remove code duplication
     * @param val New value.
     * @param expireTime Expiration time.
     * @param ttl Time to live.
     * @param ver Version to use.
     * @param topVer Topology version.
     * @param drType DR type.
     * @param mvccVer Mvcc version.
     * @param preload Flag indicating whether entry is being preloaded.
     * @throws IgniteCheckedException In case of error.
     */
    protected void finishInitialUpdate(
        GridCacheMapEntry entry,
        @Nullable CacheObject val,
        long expireTime,
        long ttl,
        GridCacheVersion ver,
        AffinityTopologyVersion topVer,
        GridDrType drType,
        MvccVersion mvccVer,
        boolean preload,
        boolean fromStore
    ) throws IgniteCheckedException {
        boolean walEnabled = !cctx.isNear() && cctx.group().persistenceEnabled() && cctx.group().walEnabled();

        entry.update(val, expireTime, ttl, ver, true);

        boolean skipQryNtf = false;

        if (val == null) {
            skipQryNtf = true;

            if (cctx.deferredDelete() && !entry.deletedUnlocked() && !entry.isInternal())
                entry.deletedUnlocked(true);
        }
        else if (entry.deletedUnlocked())
            entry.deletedUnlocked(false);

        long updateCntr = 0;

        if (!preload)
             // todo update counters is not applicable to cache entry and should be moved
            updateCntr = entry.nextPartitionCounter(topVer, true, null);

        if (walEnabled) {
            if (cctx.mvccEnabled()) {
                cctx.shared().wal().log(new MvccDataRecord(new MvccDataEntry(
                    cctx.cacheId(),
                    entry.key,
                    val,
                    val == null ? DELETE : GridCacheOperation.CREATE,
                    null,
                    ver,
                    expireTime,
                    entry.partition(),
                    updateCntr,
                    mvccVer == null ? MvccUtils.INITIAL_VERSION : mvccVer
                )));
            } else {
                cctx.shared().wal().log(new DataRecord(new DataEntry(
                    cctx.cacheId(),
                    entry.key,
                    val,
                    val == null ? DELETE : GridCacheOperation.CREATE,
                    null,
                    ver,
                    expireTime,
                    entry.partition(),
                    updateCntr
                )));
            }
        }

        entry.drReplicate(drType, val, ver, topVer);

        if (!skipQryNtf) {
            cctx.continuousQueries().onEntryUpdated(
                entry.key,
                val,
                null,
                entry.isInternal() || !entry.context().userCache(),
                entry.partition(),
                true,
                preload,
                updateCntr,
                null,
                topVer);
        }

        entry.onUpdateFinished(updateCntr);

        if (!fromStore && cctx.store().isLocal()) {
            if (val != null)
                cctx.store().put(null, entry.key, val, ver);
        }
    }


    /** */
    protected static class InitialValuePredicate implements IgnitePredicate<CacheDataRow> {
        /** */
        private final GridCacheMapEntry entry;;

        /** */
        private final boolean preload;

        /** */
        private final GridCacheVersion newVer;

        /** */
        InitialValuePredicate(GridCacheMapEntry entry, GridCacheVersion newVer, boolean preload) {
            this.entry = entry;
            this.preload = preload;
            this.newVer = newVer;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(@Nullable CacheDataRow row) {
            boolean update0;

            GridCacheVersion currentVer = row != null ? row.version() : entry.ver;

            GridCacheContext cctx = entry.cctx;

            boolean isStartVer = cctx.shared().versions().isStartVersion(currentVer);

            if (cctx.group().persistenceEnabled()) {
                if (!isStartVer) {
                    if (cctx.atomic())
                        update0 = ATOMIC_VER_COMPARATOR.compare(currentVer, newVer) < 0;
                    else
                        update0 = currentVer.compareTo(newVer) < 0;
                }
                else
                    update0 = true;
            }
            else
                update0 = isStartVer;

            update0 |= (!preload && entry.deletedUnlocked());

            return update0;
        }
    };
}
