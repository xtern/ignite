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

package org.apache.ignite.internal.visor.snapshot;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Visro task to cancel restore cache group process.
 */
@GridInternal
public class VisorSnapshotRestoreStatusTask extends VisorOneNodeTask<VisorSnapshotRestoreTaskArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorSnapshotRestoreTaskArg, String> job(VisorSnapshotRestoreTaskArg arg) {
        return new VisorSnapshotRestoreStatusJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotRestoreStatusJob extends VisorJob<VisorSnapshotRestoreTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotRestoreStatusJob(VisorSnapshotRestoreTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotRestoreTaskArg arg) throws IgniteException {
            boolean state = ignite.context().cache().context().snapshotMgr().restoreStatus(arg.snapshotName()).get();

            return "Snapshot cache group restore operation is " + (state ? "running" : "stopped") +
                " [snapshot=" + arg.snapshotName() + ']';
        }
    }
}
