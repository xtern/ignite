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

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

public class CacheEncryptionTask implements DbCheckpointListener {
    private static final int BATCH_SIZE = 10_000;

    private final GridKernalContext ctx;

    private final IgniteLogger log;

    public CacheEncryptionTask(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    private AtomicBoolean init = new AtomicBoolean();

    private Map<Integer, ReencryptionState> statesMap = new ConcurrentHashMap<>();

    private Map<Integer, IgniteInternalFuture> futMap = new ConcurrentHashMap<>();

    public IgniteInternalFuture schedule(int grpId) throws IgniteCheckedException {
        ReencryptionState state = new ReencryptionState(grpId);

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        assert grp != null;

        if (init.compareAndSet(false, true))
            ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);

        if (log.isInfoEnabled())
            log.info("Shecudled re-encryption [grp=" + grpId + "]");

        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
            if ((part.state() != OWNING && part.state() != MOVING) || part.isClearing())
                continue;

            schedule(grpId, part.id(), state);
        }

        schedule(grpId, INDEX_PARTITION, state);

//        state.fut.add(cpFut)

        state.fut.markInitialized();

        statesMap.put(grpId, state);

        futMap.put(grpId, state.fut);

        state.fut.listen(f -> {
            if (log.isInfoEnabled())
                log.info("Re-encryption is finished for group [grp=" + grpId + "]");

            boolean added = completed.offer(grpId);

            assert added;
        });

        return state.cpFut;
    }

    private void schedule(int grpId, int partId, ReencryptionState state) throws IgniteCheckedException {
        PageStore pageStore = ((FilePageStoreManager)ctx.cache().context().pageStore()).getStore(grpId, partId);

        if (pageStore.encryptedPagesCount() == 0) {
            log.info("Skipping [grpId=" + grpId + ", partId=" + partId);

            return;
        }

        GridFutureAdapter fut = new GridFutureAdapter();

        DataStoreScanner scan = new DataStoreScanner(grpId, partId, pageStore, fut);

        state.fut.add(fut);

        ctx.getSystemExecutorService().submit(scan);
    }

    public IgniteInternalFuture encryptionFuture(int grpId) {
        IgniteInternalFuture fut = futMap.get(grpId);

        return fut == null ? new GridFinishedFuture() : fut;
    }

    public IgniteInternalFuture encryptionCpFuture(int grpId) {
        ReencryptionState state = statesMap.get(grpId);

        return state == null ? new GridFinishedFuture() : state.cpFut;
    }

//    public int pageOffset(int grpId, int partId) {
//        ReencryptionState state = statesMap.get(grpId);
//
//        if (state == null)
//            return 0;
//
//        return state.offsets.get(partId == INDEX_PARTITION ? state.offsets.length() - 1 : partId);
//    }

    @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {

    }

    private void complete(int grpId) {
        ReencryptionState state = statesMap.remove(grpId);

        state.cpFut.onDone();
    }

    private final Queue<Integer> completed = new ConcurrentLinkedQueue<>();

    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        Integer grpId = null;

        Set<Integer> completeCandidates = new GridConcurrentHashSet<>();

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
                    log.warning("Checkpoint failed", e);
                }
            }
        );
    }

    @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
        //ctx.finishedStateFut()
    }

    private class ReencryptionState {
        final GridCompoundFuture fut;

        final GridFutureAdapter cpFut;

//        final AtomicIntegerArray offsets;

        public ReencryptionState(int grpId) {
            this.fut = new GridCompoundFuture();
            this.cpFut = new GridFutureAdapter();
//            this.offsets = new AtomicIntegerArray(ctx.cache().cacheGroup(grpId).topology().partitions() + 1);
        }
    }

    private class DataStoreScanner implements Callable<Void> {
        private final int grpId;

        private final int partId;

        private final int cnt;

        private final int off;

//        private final AtomicIntegerArray offsets;

        private final GridFutureAdapter fut;

        private final PageStore store;

        public DataStoreScanner(int grpId, int partId, PageStore store, GridFutureAdapter fut) {
            this.grpId = grpId;
            this.partId = partId;
//            this.offsets = offsets;
            this.fut = fut;
            this.store = store;

            cnt = store.encryptedPagesCount();
            off = store.encryptedPagesOffset();

//            updateOffset(partId, cnt);
        }

        private Void onDone() {
            store.encryptedPagesOffset(cnt);

            fut.onDone();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

            if (grp == null)
                return onDone();

            if (partId != INDEX_PARTITION) {
                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                if (part == null || part.state() != OWNING && part.state() != MOVING)
                    return onDone();
            }

            PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

            long metaPageId = pageMem.partitionMetaPageId(grpId, partId);

            int pageSize = pageMem.realPageSize(grpId);

            int pageNum = off;

            log.info("Re-encryption is started " + partId + " off=" + pageNum + ", cnt=" + cnt);

            while (pageNum < cnt) {
                if (fut.isDone())
                    break;

                ctx.cache().context().database().checkpointReadLock();

                try {
                    int end = Math.min(pageNum + BATCH_SIZE, cnt);

                    do {
                        long pageId = metaPageId + pageNum;

                        long page = pageMem.acquirePage(grpId, pageId);

                        long pageAddr = pageMem.writeLock(grpId, pageId, page, true);

                        try {
                            byte[] payload = PageUtils.getBytes(pageAddr, 0, pageSize);

                            FullPageId fullPageId = new FullPageId(pageId, grpId);

                            ctx.cache().context().wal().log(new PageSnapshot(fullPageId, payload, pageSize));
                        }
                        finally {
                            pageMem.writeUnlock(grpId, pageId, page, null, true, true);
                        }

                        pageNum++;
                    } while (pageNum < end);

                    store.encryptedPagesOffset(pageNum);
                }
                catch (Throwable t) {
                    fut.onDone(t);
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }

            log.info("Re-encryption is finished " + partId + " cnt=" + cnt);

            if (!fut.isDone())
                fut.onDone();

            return null;
        }
    }
}
