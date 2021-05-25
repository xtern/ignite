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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;

/**
 * Snapshot resore management task.
 */
@GridInternal
class SnapshotRestoreManagementTask extends ComputeTaskAdapter<IgniteCallable<Boolean>, Boolean> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        IgniteCallable<Boolean> job
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            map.put(new ComputeJobAdapter() {
                @Override public Object execute() throws IgniteException {
                    try {
                        return job.call();
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, node);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
        boolean ret = false;

        for (ComputeJobResult r : results) {
            if (r.getException() != null)
                throw new IgniteException("Failed to execute job [nodeId=" + r.getNode().id() + ']', r.getException());

            ret |= (boolean)r.getData();
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }

    /** Cancel snapshot restore operation closure. */
    @GridInternal
    protected static class CancelRestore implements IgniteCallable<Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param name Snapshot name.
         */
        CancelRestore(String name) {
            snpName = name;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws IgniteCheckedException {
            return ignite.context().cache().context().snapshotMgr().cancelLocalRestoreTask(snpName);
        }
    }

    /** Restore snapshot operation status closure. */
    @GridInternal
    protected static class RestoreStatus implements IgniteCallable<Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param name Snapshot name.
         */
        RestoreStatus(String name) {
            snpName = name;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            return ignite.context().cache().context().snapshotMgr().isRestoring(snpName);
        }
    }
}
