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


package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.PartitionAtomicUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionMvccTxUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionTxUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY;

/** */
public class ReadOnlyGridCacheDataStore implements CacheDataStore {
    /** Update counter. */
    private final PartitionUpdateCounter cntr;

    /** */
    private final IgniteLogger log;

    /** */
    private final CacheDataStore delegate;

    /** */
    private final NoopRowStore rowStore;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final GridCacheSharedContext ctx;

    /**
     * todo
     */
    public ReadOnlyGridCacheDataStore(
        CacheGroupContext grp,
        GridCacheSharedContext ctx,
        CacheDataStore delegate
    ) {
        this.grp = grp;
        this.ctx = ctx;
        this.delegate = delegate;

        log = ctx.logger(getClass());


        try {
            rowStore = new NoopRowStore(grp, new NoopFreeList(grp.dataRegion()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        if (grp.mvccEnabled())
            cntr = new PartitionMvccTxUpdateCounterImpl();
        else if (grp.hasAtomicCaches() || !grp.persistenceEnabled())
            cntr = new PartitionAtomicUpdateCounterImpl();
        else
            cntr = new PartitionTxUpdateCounterImpl();
    }

    /** {@inheritDoc} */
    @Override public synchronized void init(long updCntr) {
        if (updCntr == 0)
            resetUpdateCounter();

        updateCounter(updCntr);
    }

    /** {@inheritDoc} */
    @Override public int partId() {
        return delegate.partId();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public long cacheSize(int cacheId) {
        return delegate.cacheSize(cacheId);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> cacheSizes() {
        return delegate.cacheSizes();
    }

    /** {@inheritDoc} */
    @Override public long fullSize() {
        return delegate.fullSize();
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        delegate.updateSize(cacheId, delta);
    }

    /** {@inheritDoc} */
    @Override public long nextUpdateCounter() {
        return cntr.next();
    }

    /** {@inheritDoc} */
    @Override public long initialUpdateCounter() {
        return cntr.initial();
    }

    @Override public boolean init() {
        return delegate.init();
    }

    @Override public long reservedCounter() {
        return cntr.reserved();
    }

    @Override public @Nullable PartitionUpdateCounter partUpdateCounter() {
        return cntr;
    }

    @Override public long reserve(long delta) {
        return cntr.reserve(delta);
    }

    @Override public void updateInitialCounter(long start, long delta) {
        cntr.update(start, delta);
    }

    @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
        delegate.setRowCacheCleaner(rowCacheCleaner);
    }

    @Override public PendingEntriesTree pendingTree() {
        return delegate.pendingTree();
    }

    @Override public void resetUpdateCounter() {
        cntr.reset();
    }

    @Override public PartitionMetaStorage<SimpleDataRow> partStorage() {
        return delegate.partStorage();
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        return cntr.reserve(delta);
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return cntr.get();
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {
        try {
            cntr.update(val);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to update partition counter. " +
                "Most probably a node with most actual data is out of topology or data streamer is used " +
                "in preload mode (allowOverride=false) concurrently with cache transactions [grpName=" +
                grp.name() + ", partId=" + partId() + ']', e);

            if (Boolean.getBoolean(IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY))
                ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean updateCounter(long start, long delta) {
        return cntr.update(start, delta);
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return cntr.finalizeUpdateCounters();
    }

    @Override public void preload() throws IgniteCheckedException {
        delegate.preload();
    }

    /** {@inheritDoc} */
    @Override public void remove(
        GridCacheContext cctx,
        KeyCacheObject key,
        int partId
    ) throws IgniteCheckedException {
        // todo think
        delegate.remove(cctx, key, partId);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow createRow(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
//            assert false : "Shouldn't be here";

        assert oldRow == null;

        DataRow dataRow = makeDataRow(key, val, ver, expireTime, cctx.cacheId());

//            System.out.println(">xxx> catched " + key.value(cctx.cacheObjectContext(), false));

        // Log to the temporary store.
//            catchLog.log(new DataRecord(new DataEntry(
//                cctx.cacheId(),
//                key,
//                val,
//                val == null ? DELETE : GridCacheOperation.UPDATE,
//                null,
//                ver,
//                expireTime,
//                partId,
//                updateCounter()
//            )));

        return dataRow;
    }

    @Override public void insertRows(Collection<DataRowCacheAware> rows,
        IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException {
        // No-op.
    }

    @Override public int cleanup(GridCacheContext cctx,
        @Nullable List<MvccLinkAwareSearchRow> cleanupRows) throws IgniteCheckedException {
        // No-op.
        return 0;
    }

    @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row) throws IgniteCheckedException {
        // No-op./
    }

    @Override public void update(GridCacheContext cctx, KeyCacheObject key, CacheObject val, GridCacheVersion ver,
        long expireTime, @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
        // No-op.
    }

    @Override public boolean mvccInitialValue(GridCacheContext cctx, KeyCacheObject key, @Nullable CacheObject val,
        GridCacheVersion ver, long expireTime, MvccVersion mvccVer,
        MvccVersion newMvccVer) throws IgniteCheckedException {
        return false;
    }

    @Override public boolean mvccApplyHistoryIfAbsent(GridCacheContext cctx, KeyCacheObject key,
        List<GridCacheMvccEntryInfo> hist) throws IgniteCheckedException {
        return false;
    }

    @Override public boolean mvccUpdateRowWithPreloadInfo(GridCacheContext cctx, KeyCacheObject key,
        @Nullable CacheObject val, GridCacheVersion ver, long expireTime, MvccVersion mvccVer,
        MvccVersion newMvccVer, byte mvccTxState, byte newMvccTxState) throws IgniteCheckedException {
        return false;
    }

    @Override public MvccUpdateResult mvccUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, MvccSnapshot mvccSnapshot, @Nullable CacheEntryPredicate filter,
        EntryProcessor entryProc, Object[] invokeArgs, boolean primary, boolean needHist, boolean noCreate,
        boolean needOldVal, boolean retVal, boolean keepBinary) throws IgniteCheckedException {
        return null;
    }

    @Override
    public MvccUpdateResult mvccRemove(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter, boolean primary, boolean needHistory, boolean needOldVal,
        boolean retVal) throws IgniteCheckedException {
        // todo
        return null;
    }

    @Override public MvccUpdateResult mvccLock(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        return null;
    }

    @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void invoke(
        GridCacheContext cctx,
        KeyCacheObject key,
        IgniteCacheOffheapManager.OffheapInvokeClosure clo
    ) throws IgniteCheckedException {
        // Assume we've performed an invoke operation on the B+ Tree and find nothing.
        // Emulating that always inserting/removing a new value.
        clo.call(null);
    }

    @Override
    public void mvccApplyUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val, GridCacheVersion ver,
        long expireTime, MvccVersion mvccVer) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        return null;
    }

    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx, KeyCacheObject key,
        Object x) throws IgniteCheckedException {
        return delegate.mvccAllVersionsCursor(cctx, key, x);
    }

    @Override public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot snapshot) throws IgniteCheckedException {
        return delegate.mvccFind(cctx, key, snapshot);
    }

    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(GridCacheContext cctx,
        KeyCacheObject key) throws IgniteCheckedException {
        return delegate.mvccFindAllVersions(cctx, key);
    }

    @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
        return delegate.cursor();
    }

    @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
        return delegate.cursor(x);
    }

    @Override
    public GridCursor<? extends CacheDataRow> cursor(MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        return delegate.cursor(mvccSnapshot);
    }

    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
        return delegate.cursor(cacheId);
    }

    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        return delegate.cursor(cacheId, mvccSnapshot);
    }

    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
        KeyCacheObject upper) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper);
    }

    @Override
    public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        Object x) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper, x);
    }

    @Override
    public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        Object x, MvccSnapshot snapshot) throws IgniteCheckedException {
        return delegate.cursor(cacheId, lower, upper, x, snapshot);
    }

    @Override public void destroy() throws IgniteCheckedException {
        delegate.destroy();
    }

    @Override public void clear(int cacheId) throws IgniteCheckedException {
        delegate.clear(cacheId);
    }

    /**
     * @param key Cache key.
     * @param val Cache value.
     * @param ver Version.
     * @param expireTime Expired time.
     * @param cacheId Cache id.
     * @return Made data row.
     */
    private DataRow makeDataRow(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        int cacheId
    ) {
        if (key.partition() < 0)
            key.partition(delegate.partId());

        return new DataRow(key, val, ver, delegate.partId(), expireTime, cacheId);
    }

    /** {@inheritDoc} */
    @Override public RowStore rowStore() {
        return rowStore;
    }

    /** */
    private class NoopRowStore extends RowStore {
        /**
         * @param grp Cache group.
         * @param freeList Free list.
         */
        public NoopRowStore(CacheGroupContext grp, FreeList freeList) {
            super(grp, freeList);
        }

        /** {@inheritDoc} */
        @Override public void removeRow(long link, IoStatisticsHolder statHolder) {
            // todo
        }

        /** {@inheritDoc} */
        @Override public void addRow(CacheDataRow row, IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean updateRow(long link, CacheDataRow row, IoStatisticsHolder statHolder) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public <S, R> void updateDataRow(long link, PageHandler<S, R> pageHnd, S arg,
            IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
            // No-op.
        }
    }

    /** */
    private class NoopFreeList extends CacheFreeList {
        /** */
        public NoopFreeList(DataRegion region) throws IgniteCheckedException {
            super(0, null, null, region, null, null, 0, false, null);
        }

        /** {@inheritDoc} */
        @Override public void insertDataRow(CacheDataRow row, IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void insertDataRows(Collection<CacheDataRow> rows, IoStatisticsHolder statHolder) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean updateDataRow(long link, CacheDataRow row, IoStatisticsHolder statHolder) {
            // No-op.

            return true;
        }

        /** {@inheritDoc} */
        @Override public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) {
            // todo
        }

        /** {@inheritDoc} */
        @Override public void dumpStatistics(IgniteLogger log) {

        }

        /** {@inheritDoc} */
        @Override public Object updateDataRow(long link, PageHandler pageHnd, Object arg,
            IoStatisticsHolder statHolder) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void saveMetadata() {
            // No-op.
        }
    }
}
