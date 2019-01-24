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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractResolveSecurityContextTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the service task is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class ExecutorServiceTaskSecurityTest extends AbstractResolveSecurityContextTest {
    /** */
    @Test
    public void testExecute() {
        assertAllowed((t) -> execute(clntAllPerms, clntReadOnlyPerm, t));
        assertAllowed((t) -> execute(clntAllPerms, srvReadOnlyPerm, t));
        assertAllowed((t) -> execute(srvAllPerms, clntReadOnlyPerm, t));
        assertAllowed((t) -> execute(srvAllPerms, srvReadOnlyPerm, t));
        assertAllowed((t) -> execute(srvAllPerms, srvAllPerms, t));
        assertAllowed((t) -> execute(clntAllPerms, clntAllPerms, t));

        assertAllowed((t) -> transitionExecute(clntAllPerms, clntReadOnlyPerm, t));
        assertAllowed((t) -> transitionExecute(clntAllPerms, srvReadOnlyPerm, t));
        assertAllowed((t) -> transitionExecute(srvAllPerms, clntReadOnlyPerm, t));
        assertAllowed((t) -> transitionExecute(srvAllPerms, srvReadOnlyPerm, t));
        assertAllowed((t) -> transitionExecute(srvAllPerms, srvAllPerms, t));
        assertAllowed((t) -> transitionExecute(clntAllPerms, clntAllPerms, t));

        assertForbidden((t) -> execute(clntReadOnlyPerm, srvAllPerms, t));
        assertForbidden((t) -> execute(clntReadOnlyPerm, clntAllPerms, t));
        assertForbidden((t) -> execute(srvReadOnlyPerm, srvAllPerms, t));
        assertForbidden((t) -> execute(srvReadOnlyPerm, clntAllPerms, t));
        assertForbidden((t) -> execute(srvReadOnlyPerm, srvReadOnlyPerm, t));
        assertForbidden((t) -> execute(clntReadOnlyPerm, clntReadOnlyPerm, t));

        assertForbidden((t) -> transitionExecute(clntReadOnlyPerm, srvAllPerms, t));
        assertForbidden((t) -> transitionExecute(clntReadOnlyPerm, clntAllPerms, t));
        assertForbidden((t) -> transitionExecute(srvReadOnlyPerm, srvAllPerms, t));
        assertForbidden((t) -> transitionExecute(srvReadOnlyPerm, clntAllPerms, t));
        assertForbidden((t) -> transitionExecute(srvReadOnlyPerm, srvReadOnlyPerm, t));
        assertForbidden((t) -> transitionExecute(clntReadOnlyPerm, clntReadOnlyPerm, t));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void execute(IgniteEx initiator, IgniteEx remote, T2<String, Integer> entry) {
        try {
            initiator.executorService(initiator.cluster().forNode(remote.localNode()))
                .submit(
                    new IgniteRunnable() {
                        @Override public void run() {
                            Ignition.localIgnite().cache(CACHE_NAME)
                                .put(entry.getKey(), entry.getValue());
                        }
                    }
                ).get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void transitionExecute(IgniteEx initiator, IgniteEx remote, T2<String, Integer> entry) {
        try {
            final UUID remoteId = remote.localNode().id();

            initiator.executorService(initiator.cluster().forNode(srvTransitionAllPerms.localNode()))
                .submit(
                    new IgniteRunnable() {
                        @Override public void run() {
                            Ignite ignite = Ignition.localIgnite();

                            try {
                                ignite.executorService(ignite.cluster().forNode(ignite.cluster().node(remoteId)))
                                    .submit(new IgniteRunnable() {
                                        @Override public void run() {
                                            Ignition.localIgnite().cache(CACHE_NAME)
                                                .put(entry.getKey(), entry.getValue());
                                        }
                                    }).get();
                            }catch (Exception e){
                                throw new RuntimeException(e);
                            }
                        }
                    }
                ).get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
