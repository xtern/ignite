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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * The task for getting encryption key identifiers of the cache group.
 */
public class VisorCacheGroupKeyIdsTask extends VisorMultiNodeTask<String, Map<UUID, List<Integer>>, List<Integer>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<String, List<Integer>> job(String arg) {
        return new VisorCacheGroupKeyIdsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, List<Integer>> reduce0(List<ComputeJobResult> results) {
        Map<UUID, List<Integer>> resMap = new HashMap<>();

        for (ComputeJobResult res : results) {
            List<Integer> keyIds = res.getData();

            resMap.put(res.getNode().id(), keyIds);
        }

        return resMap;
    }

    /** The job for getting encryption key identifiers of the cache group. */
    private static class VisorCacheGroupKeyIdsJob extends VisorJob<String, List<Integer>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorCacheGroupKeyIdsJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected List<Integer> run(String arg) throws IgniteException {
            IgniteInternalCache<Object, Object> cache = ignite.context().cache().cache(arg);

            if (cache == null)
                throw new IgniteException("Cache " + arg + " not found.");

            return ignite.context().encryption().groupKeyIds(cache.context().group().groupId());
        }
    }
}
