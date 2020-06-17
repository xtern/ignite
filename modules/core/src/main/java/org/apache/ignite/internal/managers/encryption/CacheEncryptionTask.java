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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_BATCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THREAD_POOL_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THROTTLE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/**
 *
 */
public class CacheEncryptionTask implements DbCheckpointListener {
    /** Max amount of pages that will be read into memory under checkpoint lock. */
    private final int batchSize = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_BATCH_SIZE, 1_000);

    /** Timeout between batches. */
    private final long timeoutBetweenBatches = IgniteSystemProperties.getLong(IGNITE_REENCRYPTION_THROTTLE, 0);

    /** Disable background re-encryption. */
    private final boolean disabled = IgniteSystemProperties.getBoolean(IGNITE_REENCRYPTION_DISABLED, false);

    /** */
    private final int threadsCnt = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_THREAD_POOL_SIZE,
        Runtime.getRuntime().availableProcessors());

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReentrantLock initLock = new ReentrantLock();

    /** */
    private final Map<Integer, ReencryptionState> grps = new ConcurrentHashMap<>();

    /** */
    private final Queue<Integer> completedGrps = new ConcurrentLinkedQueue<>();

    /** */
    private boolean stopped;

    /** */
    private final ExecutorService execSvc = new ThreadPoolExecutor(threadsCnt, threadsCnt, DFLT_THREAD_KEEP_ALIVE_TIME,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

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

            for (ReencryptionState state : grps.values())
                state.fut.cancel();

            execSvc.shutdown();
        } finally {
            initLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        Set<Integer> completeCandidates = new HashSet<>();

        Integer grpId;

        while ((grpId = completedGrps.poll()) != null)
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
        for (ReencryptionState state : grps.values())
            state.beforeCheckpoint(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // No-op.
    }

    /**
     * @param grpId Group id.
     */
    public IgniteInternalFuture schedule(int grpId) throws IgniteCheckedException {
        ReencryptionState state = new ReencryptionState(grpId);

        if (disabled) {
            grps.put(grpId, state);

            return state.cpFut;
        }

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        if (grp == null) {
            if (log.isDebugEnabled())
                log.debug("Skip re-encryption, group was destroyed [grp=" + grpId + "]");

            return new GridFinishedFuture();
        }

        initLock.lock();

        try {
            if (stopped)
                return state.cpFut;

            if (grps.isEmpty())
                ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);

            if (log.isInfoEnabled())
                log.info("Scheduled re-encryption [grpId=" + grpId + "]");

            state.initialize(grp);

            grps.put(grpId, state);

            state.fut.listen(f -> {
                Throwable t = state.fut.error();

                if (t != null) {
                    log.error("Re-encryption is failed [grpId=" + grpId + "]", t);

                    state.cpFut.onDone(t);

                    return;
                }

                if (log.isInfoEnabled())
                    log.info("Re-encryption is finished [grpId=" + grpId + "]");

                boolean added = completedGrps.offer(grpId);

                assert added;
            });

            return state.cpFut;
        }
        finally {
            initLock.unlock();
        }
    }

    public IgniteInternalFuture encryptionFuture(int grpId) {
        ReencryptionState state = grps.get(grpId);

        return state == null ? new GridFinishedFuture() : state.fut;
    }

    public IgniteInternalFuture encryptionCpFuture(int grpId) {
        ReencryptionState state = grps.get(grpId);

        return state == null ? new GridFinishedFuture() : state.cpFut;
    }

    private void complete(int grpId) {
        ReencryptionState state = grps.remove(grpId);

        state.cpFut.onDone(state.fut.result());

        // todo sync properly
        if (!grps.isEmpty())
            return;

        initLock.lock();

        try {
            if (grps.isEmpty())
                return;

            ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).removeCheckpointListener(this);
        } finally {
            initLock.unlock();
        }
    }

    public boolean cancel(int grpId, int partId) throws IgniteCheckedException {
        ReencryptionState state = grps.get(grpId);

        if (state == null)
            return false;

        IgniteInternalFuture<Void> reencryptFut = state.futMap.get(partId);

        if (reencryptFut == null)
            return false;

        return reencryptFut.cancel();
    }

    private class ReencryptionState {
        private final int grpId;

        private ReencryptionState(int grpId) {
            this.grpId = grpId;
        }

        private final GridCompoundFuture<Void, Void> fut = new GridCompoundFuture<>();

        private final GridFutureAdapter<Void> cpFut = new GridFutureAdapter<Void>() {
            @Override public boolean cancel() throws IgniteCheckedException {
                fut.cancel();

                return onDone(null, null, true);
            }
        };

        private final Map<Integer, IgniteInternalFuture<Void>> futMap = new ConcurrentHashMap<>();

        private boolean schedulePartition(int partId) throws IgniteCheckedException {
            IgniteInternalFuture<Void> fut0 = scheduleScanner(grpId, partId, cpFinished::get);

            if (fut0 == null)
                return false;

            fut.add(fut0);

            futMap.put(partId, fut0);

            return true;
        }

        private IgniteInternalFuture<Void> scheduleScanner(int grpId, int partId, BooleanSupplier cpFinished) throws IgniteCheckedException {
            PageStore pageStore = ((FilePageStoreManager)ctx.cache().context().pageStore()).getStore(grpId, partId);

            if (pageStore.encryptedPagesCount() == 0) {
                if (log.isDebugEnabled())
                    log.debug("Skipping partition re-encryption [grp=" + grpId + ", p=" + partId + "]");

                return null;
            }

            PageStoreScanner scan = new PageStoreScanner(grpId, partId, pageStore, cpFinished);

            //ctx.getSystemExecutorService().submit(scan);
            execSvc.submit(scan);
            //ctx.closure().runLocal(scan, SYSTEM_POOL);

            return scan;
        }

        private final AtomicBoolean cpFinished = new AtomicBoolean();

        private void beforeCheckpoint(Context cpCtx) {
            if (cpFinished.get())
                return;

            cpCtx.finishedStateFut().listen(fut -> {
                cpFinished.set(true);
            });
        }

        public void initialize(CacheGroupContext grp) throws IgniteCheckedException {
            schedulePartition(INDEX_PARTITION);

            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                schedulePartition(part.id());

            fut.markInitialized();
        }
    }

    private class PageStoreScanner extends GridFutureAdapter<Void> implements Runnable {
        private final int grpId;

        private final int partId;

        private final int cnt;

        private final int off;

        private final PageStore store;

        private final Object cancelMux = new Object();

        private final BooleanSupplier cpFinished;

        public PageStoreScanner(int grpId, int partId, PageStore store, BooleanSupplier cpFinished) {
            this.grpId = grpId;
            this.partId = partId;
            this.store = store;
            this.cpFinished = cpFinished;

            cnt = store.encryptedPagesCount();
            off = store.encryptedPagesOffset();
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            synchronized (cancelMux) {
                // todo cancel properly
                return onDone();
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

                GridDhtLocalPartition part = null;

                if (partId != INDEX_PARTITION) {
                    part = grp.topology().localPartition(partId);

                    if (part == null || part.state() == GridDhtPartitionState.EVICTED) {
                        if (log.isDebugEnabled())
                            log.debug("Partition re-encryption skipped [grp=" + grpId + ", p=" + partId + "]");

                        store.encryptedPagesOffset(cnt);

                        onDone();

                        return;
                    }
                }

                PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

                long metaPageId = pageMem.partitionMetaPageId(grpId, partId);

                int pageSize = pageMem.realPageSize(grpId);

                int pageNum = off;

                if (log.isDebugEnabled()) {
                    log.debug("Partition re-encryption is started [" +
                        "p=" + partId + ", remain=" + (cnt - pageNum) + ", total=" + cnt + "]");
                }

                IgniteWriteAheadLogManager wal = ctx.cache().context().wal();

                while (pageNum < cnt) {
                    synchronized (cancelMux) {
                        ctx.cache().context().database().checkpointReadLock();

                        try {
                            int end = Math.min(pageNum + batchSize, cnt);

                            do {
                                long pageId = metaPageId + pageNum;

                                pageNum += 1;

                                long page = pageMem.acquirePage(grpId, pageId);

                                try {
                                    if (cpFinished.getAsBoolean() && pageMem.isDirty(grpId, pageId, page))
                                        continue;

                                    long pageAddr = pageMem.writeLock(grpId, pageId, page, true);

                                    boolean dirtyFlag = true;

                                    try {
                                        byte[] payload = PageUtils.getBytes(pageAddr, 0, pageSize);

                                        FullPageId fullPageId = new FullPageId(pageId, grpId);

                                        if (pageMem.isDirty(grpId, pageId, page) && !wal.disabled(grpId))
                                            wal.log(new PageSnapshot(fullPageId, payload, pageSize));
                                    }
                                    finally {
                                        pageMem.writeUnlock(grpId, pageId, page, null, dirtyFlag, false);
                                    }
                                }
                                finally {
                                    pageMem.releasePage(grpId, pageId, page);
                                }
                            }
                            while (pageNum < end);
                        }
                        finally {
                            ctx.cache().context().database().checkpointReadUnlock();
                        }
                    }

                    store.encryptedPagesOffset(pageNum);

                    if (isDone())
                        break;

                    if (timeoutBetweenBatches != 0)
                        U.sleep(timeoutBetweenBatches);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partition re-encryption is finished " +
                        "[p=" + partId +
                        ", remain=" + (cnt - pageNum) +
                        ", total=" + cnt +
                        ", cancelled=" + isCancelled() +
                        ", failed=" + isFailed() + "]");
                }
            }
            catch (Throwable t) {
                if (X.hasCause(t, NodeStoppingException.class))
                    onCancelled();

                onDone(t);
            }

            if (!isDone())
                onDone();
        }
    }
}
