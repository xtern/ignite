/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.encryption;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * The task for changing the encryption key for the cache group.
 *
 * @see IgniteEncryption#changeCacheGroupKey(Collection)
 */
public class VisorStartReencryptionTask extends VisorMultiNodeTask<Integer, Map<UUID, IgniteException>, Void> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Integer, Void> job(Integer arg) {
        return new VisorStartReencryptionJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, IgniteException> reduce0(List<ComputeJobResult> results) {
        Map<UUID, IgniteException> errs = new HashMap<>();

        for (ComputeJobResult res : results)
            errs.put(res.getNode().id(), res.getException());

        return errs;
    }

    /** The job for getting the master key name. */
    private static class VisorStartReencryptionJob extends VisorJob<Integer, Void> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorStartReencryptionJob(Integer arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Integer grpId) throws IgniteException {
            try {
                ignite.context().encryption().resumeReencryption(grpId);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }
}
