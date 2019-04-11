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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.ATOMIC_VER_COMPARATOR;
import static org.apache.ignite.internal.processors.cache.GridCacheMapEntry.IS_UNSWAPPED_MASK;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;

/** */
public class GridCacheEntryProcessor {
    /** */
    private final GridCacheContext cctx;

    /** */
    GridCacheEntryProcessor(GridCacheContext cctx) {
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    public boolean initialValue(
        GridCacheMapEntry entry,
        CacheObject val,
        GridCacheVersion ver,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte mvccTxState,
        byte newMvccTxState,
        long ttl,
        long expireTime,
        boolean preload,
        AffinityTopologyVersion topVer,
        GridDrType drType,
        boolean fromStore
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        ensureFreeSpace(entry);

        boolean deferred = false;
        boolean obsolete = false;

        GridCacheVersion oldVer = null;

        entry.lockListenerReadLock();
        entry.lockEntry();

        try {
            entry.checkObsolete();

            long expTime = expireTime < 0 ? CU.toExpireTime(ttl) : expireTime;

            val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

            final boolean unswapped = ((entry.flags & IS_UNSWAPPED_MASK) != 0);

            boolean update;

            IgnitePredicate<CacheDataRow> p = new InitialValuePredicate(entry, ver, preload);

            if (unswapped) {
                update = p.apply(null);

                if (update) {
                    // If entry is already unswapped and we are modifying it, we must run deletion callbacks for old value.
                    long oldExpTime = entry.expireTimeUnlocked();

                    if (oldExpTime > 0 && oldExpTime < U.currentTimeMillis()) {
                        if (onExpired(entry, entry.val, null)) {
                            if (cctx.deferredDelete()) {
                                deferred = true;
                                oldVer = entry.ver;
                            }
                            else if (val == null)
                                obsolete = true;
                        }
                    }

                    if (cctx.mvccEnabled()) {
                        assert !preload;

                        cctx.offheap().mvccInitialValue(entry, val, ver, expTime, mvccVer, newMvccVer);
                    }
                    else
                        storeValue(entry, val, expTime, ver, null);
                }
            }
            else {
                if (cctx.mvccEnabled()) {
                    // cannot identify whether the entry is exist on the fly
                    entry.unswap(false);

                    if (update = p.apply(null)) {
                        // If entry is already unswapped and we are modifying it, we must run deletion callbacks for old value.
                        long oldExpTime = entry.expireTimeUnlocked();
                        long delta = (oldExpTime == 0 ? 0 : oldExpTime - U.currentTimeMillis());

                        if (delta < 0) {
                            if (onExpired(entry, entry.val, null)) {
                                if (cctx.deferredDelete()) {
                                    deferred = true;
                                    oldVer = entry.ver;
                                }
                                else if (val == null)
                                    obsolete = true;
                            }
                        }

                        assert !preload;

                        cctx.offheap().mvccInitialValue(entry, val, ver, expTime, mvccVer, newMvccVer);
                    }
                }
                else
                    // Optimization to access storage only once.
                    update = storeValue(entry, val, expTime, ver, p);
            }

            if (update) {
                finishInitialUpdate(entry, val, expireTime, ttl, ver, topVer, drType, mvccVer, preload);

                return true;
            }

            return false;
        }
        finally {
            entry.unlockEntry();
            entry.unlockListenerReadLock();

            // It is necessary to execute these callbacks outside of lock to avoid deadlocks.

            if (obsolete) {
                entry.onMarkedObsolete();

                cctx.cache().removeEntry(entry);
            }

            if (deferred) {
                assert oldVer != null;

                cctx.onDeferredDelete(entry, oldVer);
            }
        }
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
        boolean preload
    ) throws IgniteCheckedException {
        boolean fromStore = false;
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

        drReplicate(entry, drType, val, ver, topVer);

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

    /**
     * Stores value in off-heap.
     *
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @param predicate Optional predicate.
     * @return {@code True} if storage was modified.
     * @throws IgniteCheckedException If update failed.
     */
    protected boolean storeValue(
        GridCacheMapEntry entry,
        @Nullable CacheObject val,
        long expireTime,
        GridCacheVersion ver,
        @Nullable IgnitePredicate<CacheDataRow> predicate) throws IgniteCheckedException {
        assert entry.lock.isHeldByCurrentThread();

        GridCacheMapEntry.UpdateClosure closure = new GridCacheMapEntry.UpdateClosure(entry, val, ver, expireTime, predicate);

        cctx.offheap().invoke(cctx, entry.key(), entry.localPartition(), closure);

        return closure.treeOp != IgniteTree.OperationType.NOOP;
    }

    /**
     * Perform DR if needed.
     *
     * @param drType DR type.
     * @param val Value.
     * @param ver Version.
     * @param topVer Topology version.
     * @throws IgniteCheckedException In case of exception.
     */
    private void drReplicate(GridCacheMapEntry entry, GridDrType drType, @Nullable CacheObject val, GridCacheVersion ver, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        if (cctx.isDrEnabled() && drType != DR_NONE && !entry.isInternal())
            cctx.dr().replicate(entry.key, val, entry.rawTtl(), entry.rawExpireTime(), ver.conflictVersion(), drType, topVer);
    }

    /**
     * Evicts necessary number of data pages if per-page eviction is configured in current {@link DataRegion}.
     */
    private void ensureFreeSpace(GridCacheMapEntry entry) throws IgniteCheckedException {
        // Deadlock alert: evicting data page causes removing (and locking) all entries on the page one by one.
        assert !entry.lock.isHeldByCurrentThread();

        cctx.shared().database().ensureFreeSpace(cctx.dataRegion());
    }

    /**
     * @param expiredVal Expired value.
     * @param obsoleteVer Version.
     * @return {@code True} if entry was marked as removed.
     * @throws IgniteCheckedException If failed.
     */
    private boolean onExpired(GridCacheMapEntry entry, CacheObject expiredVal, GridCacheVersion obsoleteVer) throws IgniteCheckedException {
        assert expiredVal != null;

        boolean rmvd = false;

        if (entry.mvccExtras() != null)
            return false;

        if (cctx.deferredDelete() && !entry.detached() && !entry.isInternal()) {
            if (!entry.deletedUnlocked() && !entry.isStartVersion()) {
                entry.update(null, 0L, 0L, entry.ver, true);

                entry.deletedUnlocked(true);

                rmvd = true;
            }
        }
        else {
            if (obsoleteVer == null)
                obsoleteVer = cctx.versions().next(entry.ver);

            if (entry.markObsolete0(obsoleteVer, true, null))
                rmvd = true;
        }

//        if (log.isTraceEnabled())
//            log.trace("onExpired clear [key=" + key + ", entry=" + System.identityHashCode(this) + ']');

        cctx.shared().database().checkpointReadLock();

        try {
            if (cctx.mvccEnabled())
                cctx.offheap().mvccRemoveAll(entry);
            else
                removeValue(entry);
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }

        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
            cctx.events().addEvent(entry.partition(),
                entry.key,
                cctx.localNodeId(),
                null,
                EVT_CACHE_OBJECT_EXPIRED,
                null,
                false,
                expiredVal,
                expiredVal != null,
                null,
                null,
                null,
                true);
        }

        cctx.continuousQueries().onEntryExpired(entry, entry.key, expiredVal);

        return rmvd;
    }

    /**
     * Removes value from offheap.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void removeValue(GridCacheMapEntry entry) throws IgniteCheckedException {
        assert entry.lock.isHeldByCurrentThread();

        cctx.offheap().remove(cctx, entry.key, entry.partition(), entry.localPartition());
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
