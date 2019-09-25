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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRowCacheAware;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * <p>
 *     This is the CacheDataStoreEx implementation. The main purpose is hot switching between different
 *     modes of cache data storage (e.g. between <tt>FULL</tt> and <tt>LOG_ONLY</tt> mode) to guarantee the
 *     consistency for Checkpointer writes and async cache put operations.
 * </p>
 */
public class CacheDataStoreExImpl implements CacheDataStoreEx {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Currently used data storage state. <tt>FULL</tt> mode is used by default. */
    private final AtomicBoolean readOnly = new AtomicBoolean();

    /** */
    private final CacheDataStore store;

    /** */
    private final CacheDataStore readOnlyStore;

    /**
     * @param primary The main storage to perform full cache operations.
     * @param secondary The storage to handle only write operation in temporary mode.
     */
    public CacheDataStoreExImpl(
        GridCacheSharedContext<?, ?> cctx,
        CacheDataStore primary,
        CacheDataStore secondary,
        IgniteLogger log
    ) {
        assert primary != null;

        this.cctx = cctx;
        this.log = log;

        store = primary;
        readOnlyStore = secondary;
    }

    /** {@inheritDoc} */
    @Override public CacheDataStore store(boolean readOnly) {
        return readOnly ? readOnlyStore : store;
    }

    /** {@inheritDoc} */
    @Override public void readOnly(boolean readOnly) {
        assert cctx.database().checkpointLockIsHeldByThread() : "Changing mode required checkpoint write lock";

        if (this.readOnly.compareAndSet(!readOnly, readOnly))
            log.info("Changing data store mode to " + (readOnly ? "READ-REMOVE" : "FULL") + " [p=" + partId() + "]");
    }

    /** {@inheritDoc} */
    @Override public boolean readOnly() {
        return readOnly.get();
    }

    /**
     * @return The currently active cache data storage.
     */
    private CacheDataStore activeStorage() {
        return store(readOnly.get());
    }

    /** {@inheritDoc} */
    @Override public boolean init() {
        return activeStorage().init();
    }

    /** {@inheritDoc} */
    @Override public int partId() {
        return activeStorage().partId();
    }

    /** {@inheritDoc} */
    @Override public void reinit() {
        activeStorage().reinit();
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
        return activeStorage().createRow(cctx, key, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public void insertRows(Collection<DataRowCacheAware> rows,
        IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException {
        activeStorage().insertRows(rows, initPred);
    }

    /** {@inheritDoc} */
    @Override public int cleanup(
        GridCacheContext cctx,
        @Nullable List<MvccLinkAwareSearchRow> cleanupRows
    ) throws IgniteCheckedException {
        return activeStorage().cleanup(cctx, cleanupRows);
    }

    /** {@inheritDoc} */
    @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row) throws IgniteCheckedException {
        activeStorage().updateTxState(cctx, row);
    }

    /** {@inheritDoc} */
    @Override public void update(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
        activeStorage().update(cctx, key, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccInitialValue(
        GridCacheContext cctx,
        KeyCacheObject key,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer
    ) throws IgniteCheckedException {
        return activeStorage().mvccInitialValue(cctx, key, val, ver, expireTime, mvccVer, newMvccVer);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccApplyHistoryIfAbsent(
        GridCacheContext cctx,
        KeyCacheObject key,
        List<GridCacheMvccEntryInfo> hist
    ) throws IgniteCheckedException {
        return activeStorage().mvccApplyHistoryIfAbsent(cctx, key, hist);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccUpdateRowWithPreloadInfo(
        GridCacheContext cctx,
        KeyCacheObject key,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte mvccTxState,
        byte newMvccTxState
    ) throws IgniteCheckedException {
        return activeStorage().mvccUpdateRowWithPreloadInfo(
            cctx, key, val, ver, expireTime, mvccVer, newMvccVer, mvccTxState, newMvccTxState
        );
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccUpdate(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter,
        EntryProcessor entryProc,
        Object[] invokeArgs,
        boolean primary,
        boolean needHist,
        boolean noCreate,
        boolean needOldVal,
        boolean retVal,
        boolean keepBinary
    ) throws IgniteCheckedException {
        return activeStorage().mvccUpdate(
            cctx, key, val, ver, expireTime, mvccSnapshot, filter, entryProc, invokeArgs, primary, needHist, noCreate,
            needOldVal, retVal, keepBinary
        );
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccRemove(
        GridCacheContext cctx,
        KeyCacheObject key,
        MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter,
        boolean primary,
        boolean needHistory,
        boolean needOldVal,
        boolean retVal
    ) throws IgniteCheckedException {
        return activeStorage().mvccRemove(cctx, key, mvccSnapshot, filter, primary, needHistory, needOldVal, retVal);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccLock(
        GridCacheContext cctx,
        KeyCacheObject key,
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        return activeStorage().mvccLock(cctx, key, mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        activeStorage().mvccRemoveAll(cctx, key);
    }

    /** {@inheritDoc} */
    @Override public void invoke(
        GridCacheContext cctx,
        KeyCacheObject key,
        IgniteCacheOffheapManager.OffheapInvokeClosure c
    ) throws IgniteCheckedException {
        // todo should be executed under read lock?
        activeStorage().invoke(cctx, key, c);
    }

    /** {@inheritDoc} */
    @Override public void mvccApplyUpdate(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer
    ) throws IgniteCheckedException {
        activeStorage().mvccApplyUpdate(cctx, key, val, ver, expireTime, mvccVer);
    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException {
        activeStorage().remove(cctx, key, partId);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        return activeStorage().find(cctx, key);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(
        GridCacheContext cctx,
        KeyCacheObject key,
        Object x
    ) throws IgniteCheckedException {
        return activeStorage().mvccAllVersionsCursor(cctx, key, x);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow mvccFind(
        GridCacheContext cctx,
        KeyCacheObject key,
        MvccSnapshot snapshot
    ) throws IgniteCheckedException {
        return activeStorage().mvccFind(cctx, key, snapshot);
    }

    /** {@inheritDoc} */
    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(
        GridCacheContext cctx,
        KeyCacheObject key
    ) throws IgniteCheckedException {
        return activeStorage().mvccFindAllVersions(cctx, key);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
        return activeStorage().cursor();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
        return activeStorage().cursor(x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        return activeStorage().cursor(mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
        return activeStorage().cursor(cacheId);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        return activeStorage().cursor(cacheId, mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper
    ) throws IgniteCheckedException {
        return activeStorage().cursor(cacheId, lower, upper);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper,
        Object x
    ) throws IgniteCheckedException {
        return activeStorage().cursor(cacheId, lower, upper, x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper,
        Object x,
        MvccSnapshot snapshot
    ) throws IgniteCheckedException {
        return activeStorage().cursor(cacheId, lower, upper, x, snapshot);
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        activeStorage().destroy();
    }

    /** {@inheritDoc} */
    @Override public void clear(int cacheId) throws IgniteCheckedException {
        activeStorage().clear(cacheId);
    }

    /** {@inheritDoc} */
    @Override public RowStore rowStore() {
        // Checkpointer must always have assess to the original storage.
        return activeStorage().rowStore();
    }

    /** {@inheritDoc} */
    @Override public void updateInitialCounter(long start, long delta) {
        activeStorage().updateInitialCounter(start, delta);
    }

    /** {@inheritDoc} */
    @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
        activeStorage().setRowCacheCleaner(rowCacheCleaner);
    }

    /** {@inheritDoc} */
    @Override public PendingEntriesTree pendingTree() {
        return activeStorage().pendingTree();
    }

    /** {@inheritDoc} */
    @Override public void preload() throws IgniteCheckedException {
        activeStorage().preload();
    }

    /** {@inheritDoc} */
    @Override public void resetUpdateCounter() {
        activeStorage().resetUpdateCounter();
    }

    /** {@inheritDoc} */
    @Override public PartitionMetaStorage<SimpleDataRow> partStorage() {
        return activeStorage().partStorage();
    }

    /** {@inheritDoc} */
    @Override public long cacheSize(int cacheId) {
        return activeStorage().cacheSize(cacheId);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> cacheSizes() {
        return activeStorage().cacheSizes();
    }

    /** {@inheritDoc} */
    @Override public long fullSize() {
        return activeStorage().fullSize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return activeStorage().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        activeStorage().updateSize(cacheId, delta);
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return activeStorage().updateCounter();
    }

    /** {@inheritDoc} */
    @Override public long reservedCounter() {
        return activeStorage().reservedCounter();
    }

    /** {@inheritDoc} */
    @Override public @Nullable PartitionUpdateCounter partUpdateCounter() {
        return activeStorage().partUpdateCounter();
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        return activeStorage().reserve(delta);
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {
        activeStorage().updateCounter(val);
    }

    /** {@inheritDoc} */
    @Override public boolean updateCounter(long start, long delta) {
        return activeStorage().updateCounter(start, delta);
    }

    /** {@inheritDoc} */
    @Override public long nextUpdateCounter() {
        return activeStorage().nextUpdateCounter();
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        return activeStorage().getAndIncrementUpdateCounter(delta);
    }

    /** {@inheritDoc} */
    @Override public long initialUpdateCounter() {
        return activeStorage().initialUpdateCounter();
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return activeStorage().finalizeUpdateCounters();
    }
}
