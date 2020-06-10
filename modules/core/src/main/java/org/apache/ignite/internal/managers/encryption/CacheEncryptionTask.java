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

package org.apache.ignite.internal.managers.encryption;

import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_BATCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THROTTLE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 *
 */
public class CacheEncryptionTask implements DbCheckpointListener {
    /** Max amount of pages that will be read into memory under checkpoint lock. */
    private final int batchSize = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_BATCH_SIZE, 10_000);

    /** Timeout between batches. */
    private final long timeoutBetweenBatches = IgniteSystemProperties.getLong(IGNITE_REENCRYPTION_THROTTLE, 0);

    /** Disable background re-encryption. */
    private final boolean disabled = IgniteSystemProperties.getBoolean(IGNITE_REENCRYPTION_DISABLED, false);

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReentrantLock initLock = new ReentrantLock();

    /** */
    private final Map<Integer, ReencryptionState> statesMap = new ConcurrentHashMap<>();

    /** */
    private final Queue<Integer> completed = new ConcurrentLinkedQueue<>();

    /** */
    private boolean stopped;

    /**
     * @param ctx Grid kernal context.
     */
    public CacheEncryptionTask(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /**
     * Shutdown re-encryption and disable new tasks scheduling.
     */
    public void stop() throws IgniteCheckedException {
        initLock.lock();

        try {
            stopped = true;

            for (ReencryptionState state : statesMap.values())
                state.fut.cancel();
        } finally {
            initLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        Set<Integer> completeCandidates = new HashSet<>();

        Integer grpId;

        while ((grpId = completed.poll()) != null)
            completeCandidates.add(grpId);

        ctx.finishedStateFut().listen(
            f -> {
                try {
                    f.get();

                    for (int grpId0 : completeCandidates)
                        complete(grpId0);
                }
                catch (IgniteCheckedException e) {
                    log.warning("Checkpoint failed.", e);
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // No-op.
    }

    /**
     * @param grpId Group id.
     */
    public IgniteInternalFuture schedule(int grpId) throws IgniteCheckedException {
        ReencryptionState state = new ReencryptionState();

        if (disabled) {
            statesMap.put(grpId, state);

            return state.cpFut;
        }

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        assert grp != null;

        initLock.lock();

        try {
            if (stopped)
                return state.cpFut;

            if (statesMap.isEmpty())
                ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);

            if (log.isInfoEnabled())
                log.info("Scheduled re-encryption [grp=" + grpId + "]");

            IgniteInternalFuture<Void> fut = scheduleScanner(grpId, INDEX_PARTITION);

            if (fut != null)
                state.fut.add(fut);

            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
//                if ((part.state() != OWNING && part.state() != MOVING) || part.isClearing())
//                    continue;
                if (part.state() == EVICTED)
                    continue;

                IgniteInternalFuture<Void> fut0 = scheduleScanner(grpId, part.id());

                if (fut0 != null)
                    state.fut.add(fut0);
            }

            state.fut.markInitialized();

            statesMap.put(grpId, state);

            state.fut.listen(f -> {
                Throwable t = state.fut.error();

                if (t != null) {
                    log.error("Re-encryption is failed [grp=" + grpId + "]", t);

                    state.cpFut.onDone(t);

                    return;
                }

                if (log.isInfoEnabled())
                    log.info("Re-encryption is finished [grp=" + grpId + "]");

                boolean added = completed.offer(grpId);

                assert added;
            });

            return state.cpFut;
        }
        finally {
            initLock.unlock();
        }
    }

    public IgniteInternalFuture encryptionFuture(int grpId) {
        ReencryptionState state = statesMap.get(grpId);

        return state == null ? new GridFinishedFuture() : state.fut;
    }

    public IgniteInternalFuture encryptionCpFuture(int grpId) {
        ReencryptionState state = statesMap.get(grpId);

        return state == null ? new GridFinishedFuture() : state.cpFut;
    }

    private IgniteInternalFuture<Void> scheduleScanner(int grpId, int partId) throws IgniteCheckedException {
        PageStore pageStore = ((FilePageStoreManager)ctx.cache().context().pageStore()).getStore(grpId, partId);

        if (pageStore.encryptedPagesCount() == 0) {
            if (log.isDebugEnabled())
                log.debug("Skipping re-encryption for part [grpId=" + grpId + ", partId=" + partId);

            return null;
        }

//        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        PageStoreScanner scan = new PageStoreScanner(grpId, partId, pageStore);

        ctx.getSystemExecutorService().submit(scan);
        //ctx.closure().runLocal(scan, SYSTEM_POOL);

        return scan;
    }

    private void complete(int grpId) {
        ReencryptionState state = statesMap.remove(grpId);

        state.cpFut.onDone(state.fut.result());

        // todo sync properly
        if (!statesMap.isEmpty())
            return;

        initLock.lock();

        try {
            if (statesMap.isEmpty())
                return;

            ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).removeCheckpointListener(this);
        } finally {
            initLock.unlock();
        }
    }

    public boolean cancel(int grpId, int partId) throws IgniteCheckedException {
        ReencryptionState state = statesMap.get(grpId);

        if (state == null)
            return false;

        IgniteInternalFuture<Void> reencryptFut = state.futMap.get(partId);

        if (reencryptFut == null)
            return false;

        return reencryptFut.cancel();
    }

    private static class ReencryptionState {
        private final GridCompoundFuture<Void, Void> fut = new GridCompoundFuture<>();

        private final GridFutureAdapter<Void> cpFut = new GridFutureAdapter<>();

        private final Map<Integer, IgniteInternalFuture<Void>> futMap = new ConcurrentHashMap<>();
    }

    private class PageStoreScanner extends GridFutureAdapter<Void> implements Runnable {
        private final int grpId;

        private final int partId;

        private final int cnt;

        private final int off;

        private final PageStore store;

        private final Object cancelMux = new Object();

        public PageStoreScanner(int grpId, int partId, PageStore store) {
            this.grpId = grpId;
            this.partId = partId;
            this.store = store;

            cnt = store.encryptedPagesCount();
            off = store.encryptedPagesOffset();
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            synchronized (cancelMux) {
                return onDone(null, null, true);
            }
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

                if (grp == null) {
                    onDone();

                    return;
                }

                if (partId != INDEX_PARTITION) {
                    GridDhtLocalPartition part = grp.topology().localPartition(partId);

                    if (part == null || part.state() == EVICTED) {
                        store.encryptedPagesOffset(cnt);

                        onDone();

                        return;
                    }

                    if (part.state() == RENTING || part.isClearing()) {
                        // todo reencrypt metapage
                        part.onClearFinished(f -> {
                            store.encryptedPagesOffset(cnt);

                            onDone();
                        });
                    }
                }

                PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

                long metaPageId = pageMem.partitionMetaPageId(grpId, partId);

                int pageSize = pageMem.realPageSize(grpId);

                int pageNum = off;

                if (log.isDebugEnabled()) {
                    log.debug("Partition re-encryption is started [" +
                        "partId=" + partId + ", offset=" + pageNum + ", total=" + cnt + "]");
                }

                IgniteWriteAheadLogManager wal = ctx.cache().context().wal();

                while (pageNum < cnt) {
                    synchronized (cancelMux) {
                        ctx.cache().context().database().checkpointReadLock();

                        try {
                            int end = Math.min(pageNum + batchSize, cnt);

                            do {
                                long pageId = metaPageId + pageNum;

                                long page = pageMem.acquirePage(grpId, pageId);

                                long pageAddr = pageMem.writeLock(grpId, pageId, page, true);

                                try {
                                    byte[] payload = PageUtils.getBytes(pageAddr, 0, pageSize);

                                    FullPageId fullPageId = new FullPageId(pageId, grpId);

                                    if (pageMem.isDirty(grpId, pageId, page) && !wal.disabled(grpId))
                                        wal.log(new PageSnapshot(fullPageId, payload, pageSize));
                                }
                                finally {
                                    pageMem.writeUnlock(grpId, pageId, page, null, true, true);
                                }

                                pageNum++;
                            }
                            while (pageNum < end);
                        }
                        finally {
                            ctx.cache().context().database().checkpointReadUnlock();
                        }
                    }

                    store.encryptedPagesOffset(pageNum);

                    if (isDone()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Partition re-encryption was interrupted [" +
                                "grpId=" + grpId + ", partId=" + partId + ", off=" + pageNum + ", total=" + cnt);
                        }

                        break;
                    }

                    if (timeoutBetweenBatches != 0)
                        U.sleep(timeoutBetweenBatches);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partition re-encryption is finished [" +
                        "partId=" + partId + ", offset=" + pageNum + ", total=" + cnt + "]");
                }
            }
            catch (Throwable t) {
                onDone(t);
            }

            if (!isDone())
                onDone();
        }
    }
}
