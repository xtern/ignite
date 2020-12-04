package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT_RESTORE;

public class RestoreSnapshotProcess {
    private final GridKernalContext ctx;

    private volatile SnapshotRestoreRequest req;

    private volatile RestoreSnapshotFuture fut;

//    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> prepareRestoreProc;
    private final DistributedProcess<SnapshotRestoreRequest, SnapshotRestoreResponse> performRestoreProc;

    public RestoreSnapshotProcess(GridKernalContext ctx) {
        this.ctx = ctx;

//        prepareRestoreProc = new DistributedProcess<>(ctx, START_SNAPSHOT_RESTORE, this::prepare, this::finishPrepare);
        performRestoreProc = new DistributedProcess<>(ctx, END_SNAPSHOT_RESTORE, this::perform, this::finishPerform);
    }

    public IgniteFuture<Void> start(String snpName, Collection<String> cacheOrGrpNames) {
        if (ctx.state().clusterState().state() != ClusterState.INACTIVE)
            throw new IgniteException("Operation was rejected. The cluster should be inactive.");

        IgniteInternalFuture<Void> fut0 = fut;

        if (fut0 != null && !fut0.isDone()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Snapshot restore operation was rejected. " +
                "The previous snapshot restore operation was not completed."));
        }

        SnapshotRestoreRequest req = new SnapshotRestoreRequest(snpName, cacheOrGrpNames);

        // todo cas?
        fut = new RestoreSnapshotFuture(req.requestId());

        performRestoreProc.start(req.requestId(), req);

        return new IgniteFutureImpl<>(fut);
    }

    public boolean inProgress() {
        return req != null;
    }

    private IgniteInternalFuture<SnapshotRestoreResponse> perform(SnapshotRestoreRequest req) {
        this.req = req;
//        if (!req.equals(this.req))
//            return new GridFinishedFuture<>(new IgniteException("Unknown snapshot restore operation was rejected."));

        try {
            if (!ctx.clientNode())
                ctx.cache().context().snapshotMgr().restoreCacheGroupsLocal(req.snapshotName(), req.groups());
        } catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        return new GridFinishedFuture<>(new SnapshotRestoreResponse());
    }

    private void finishPerform(UUID id, Map<UUID, SnapshotRestoreResponse> map, Map<UUID, Exception> errs) {
        boolean isInitiator = fut != null && fut.id().equals(id);

        if (!isInitiator || fut.isDone())
            return;

        if (F.isEmpty(errs))
            fut.onDone();
        else
            fut.onDone(F.firstValue(errs));

        req = null;
    }

    protected static class RestoreSnapshotFuture extends GridFutureAdapter<Void> {
        /** Request ID. */
        private final UUID id;

        /** @param id Request ID. */
        RestoreSnapshotFuture(UUID id) {
            this.id = id;
        }

        /** @return Request ID. */
        public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RestoreSnapshotFuture.class, this);
        }
    }


//    private void finishPrepare(UUID uuid, Map<UUID, SnapshotRestoreResponse> map, Map<UUID, Exception> map1) {
//
//    }
//
//    private IgniteInternalFuture<SnapshotRestoreResponse> prepare(SnapshotRestoreRequest request) {
//        return null;
//    }
}
