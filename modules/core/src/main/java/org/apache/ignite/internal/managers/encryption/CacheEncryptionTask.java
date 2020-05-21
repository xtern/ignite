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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

public class CacheEncryptionTask {
    private static final int BATCH_SIZE = 10_000;

    private final GridKernalContext ctx;

    public CacheEncryptionTask(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    private Map<Integer, ReencryptionState> statesMap = new ConcurrentHashMap<>();

    public IgniteInternalFuture schedule(int grpId) throws IgniteCheckedException {
        ReencryptionState state = new ReencryptionState(grpId);

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        assert grp != null;

        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
            if (part.state() != OWNING && part.state() != MOVING)
                continue;

            schedule(grpId, part.id(), state);
        }

        schedule(grpId, INDEX_PARTITION, state);

        state.fut.markInitialized();

        statesMap.put(grpId, state);

        return state.fut;
    }

    private void schedule(int grpId, int partId, ReencryptionState state) throws IgniteCheckedException {
        PageStore pageStore = ((FilePageStoreManager)ctx.cache().context().pageStore()).getStore(grpId, partId);

        if (pageStore.encryptedPagesCount() == 0)
            return;

        GridFutureAdapter fut = new GridFutureAdapter();

        DataStoreScanner scan = new DataStoreScanner(grpId, partId, pageStore, state.offsets, fut);

        state.fut.add(fut);

        ctx.getSystemExecutorService().submit(scan);
    }

    public int pageOffset(int grpId, int partId) {
        ReencryptionState state = statesMap.get(grpId);

        if (state == null)
            return 0;

        return state.offsets.get(partId == INDEX_PARTITION ? state.offsets.length() - 1 : partId);
    }

    private class ReencryptionState {
        final GridCompoundFuture fut;

        final AtomicIntegerArray offsets;

        public ReencryptionState(int grpId) {
            this.fut = new GridCompoundFuture();
            this.offsets = new AtomicIntegerArray(ctx.cache().cacheGroup(grpId).topology().partitions() + 1);
        }
    }

    private class DataStoreScanner implements Callable<Void> {
        private final int grpId;

        private final int partId;

        private final int cnt;

        private final int off;

        private final AtomicIntegerArray offsets;

        private final GridFutureAdapter fut;

        public DataStoreScanner(int grpId, int partId, PageStore store, AtomicIntegerArray offsets, GridFutureAdapter fut) {
            this.grpId = grpId;
            this.partId = partId;
            this.offsets = offsets;
            this.fut = fut;

            cnt = store.encryptedPagesCount();
            off = store.encryptedPagesOffset();

            updateOffset(partId, cnt);
        }

        private void updateOffset(int idx, int val) {
            if (idx == INDEX_PARTITION)
                idx = offsets.length() - 1;

            offsets.set(idx, val);
        }

        private Void onDone() {
            updateOffset(partId, cnt);

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

            ctx.cache().context().database().checkpointReadLock();

            try {
                for (int pageNum = off; pageNum < cnt; pageNum++) {
                    if (fut.isDone())
                        break;

                    long pageId = metaPageId + pageNum;

                    long page = pageMem.acquirePage(grpId, pageId);

                    pageMem.writeLock(grpId, pageId, page, true);

                    pageMem.writeUnlock(grpId, pageId, page, null, true, true);

                    updateOffset(partId, pageNum);
                }
            }
            catch (Throwable t) {
                fut.onDone(t);
            }
            finally {
                ctx.cache().context().database().checkpointReadUnlock();
            }

            if (!fut.isDone())
                fut.onDone();

            return null;
        }
    }
}
