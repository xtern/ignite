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

package org.apache.ignite.internal.processor.security.compute.closure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Testing permissions when the compute task is executed cache operations on remote node.
 */
public class ComputeTaskSecurityTest extends AbstractComputeTaskSecurityTest {
    /** {@inheritDoc} */
    @Override protected void checkSuccess(IgniteEx initiator, IgniteEx remote) {
        final UUID remoteId = remote.localNode().id();

        successCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.execute(new TestComputeTask(remoteId, k, v), 0)
        );
        successCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.executeAsync(new TestComputeTask(remoteId, k, v), 0).get()
        );

        final UUID transitionId = srvTransitionAllPerms.localNode().id();

        successCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.execute(new TestTransitionComputeTask(transitionId, remoteId, k, v), 0)
        );
        successCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.executeAsync(new TestTransitionComputeTask(transitionId, remoteId, k, v), 0).get()
        );
    }

    /** {@inheritDoc} */
    @Override protected void checkFail(IgniteEx initiator, IgniteEx remote) {
        final UUID remoteId = remote.localNode().id();

        failCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.execute(new TestComputeTask(remoteId, k, v), 0)
        );
        failCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.executeAsync(new TestComputeTask(remoteId, k, v), 0).get()
        );

        final UUID transitionId = srvTransitionAllPerms.localNode().id();

        failCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.execute(new TestTransitionComputeTask(transitionId, remoteId, k, v), 0)
        );
        failCompute(
            initiator, remote,
            (cmp, k, v) ->
                cmp.executeAsync(new TestTransitionComputeTask(transitionId, remoteId, k, v), 0).get()
        );
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successCompute(IgniteEx initiator, IgniteEx remote,
        C3<IgniteCompute, String, Integer> consumer) {
        int val = values.getAndIncrement();

        consumer.accept(initiator.compute(), "key", val);

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failCompute(IgniteEx initiator, IgniteEx remote,
        C3<IgniteCompute, String, Integer> consumer) {
        assertCauseSecurityException(
            GridTestUtils.assertThrowsWithCause(
                () -> consumer.accept(initiator.compute(), "fail_key", -1)
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }

    /**
     * Compute task for tests.
     */
    static class TestComputeTask implements ComputeTask<Integer, Integer> {
        /** Remote cluster node. */
        protected final UUID remote;

        /** Key. */
        protected final String key;

        /** Value. */
        protected final Integer val;

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remote Remote.
         * @param key Key.
         * @param val Value.
         */
        public TestComputeTask(UUID remote, String key, Integer val) {
            this.remote = remote;
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Integer arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> res = new HashMap<>();

            res.put(
                new ComputeJob() {
                    @IgniteInstanceResource
                    private Ignite loc;

                    @Override public void cancel() {
                        // no-op
                    }

                    @Override public Object execute() throws IgniteException {
                        loc.cache(CACHE_NAME).put(key, val);

                        return null;
                    }
                }, loc.cluster().node(remote)
            );

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> rcvd) throws IgniteException {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /**
     * Transition compute task for tests.
     */
    static class TestTransitionComputeTask extends TestComputeTask {
        /** Transition cluster node. */
        private final UUID transition;

        /**
         * @param transition Transition.
         * @param remote Remote.
         * @param key Key.
         * @param val Value.
         */
        public TestTransitionComputeTask(UUID transition, UUID remote, String key, Integer val) {
            super(remote, key, val);

            this.transition = transition;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Integer arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> res = new HashMap<>();

            res.put(
                new ComputeJob() {
                    @IgniteInstanceResource
                    private Ignite loc;

                    @Override public void cancel() {
                        // no-op
                    }

                    @Override public Object execute() throws IgniteException {
                        loc.compute().execute(
                            new TestComputeTask(remote, key, val), 0
                        );

                        return null;
                    }
                }, loc.cluster().node(transition)
            );

            return res;
        }
    }
}