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

package org.apache.ignite.util.mbeans;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.cluster.IgniteTopologyMXBeanImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test ignite topology management bean.
 */
public class IgniteTopologyMXBeanTest extends GridCommonAbstractTest {
    /** Count of server nodes. */
    private static final int SERVER_NODES = 3;

    /** Count of client nodes. */
    private static final int CLIENT_NODES = 2;

    /** */
    private static final int TOTAL_NODES = SERVER_NODES + CLIENT_NODES;

    /** */
    private static final String ATTR0 = "attr0";

    /** */
    private static final String ATTR1 = "attr1";

    /** */
    private static final String VAL0 = "val0";

    /** */
    private static final String VAL1 = "val1";

    /** */
    private boolean client;

    /** */
    private int gridCntr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setClientMode(client);
        configuration.setUserAttributes(
            Collections.singletonMap(gridCntr % 2 == 0 ? ATTR0 : ATTR1, ++gridCntr < SERVER_NODES ? VAL0 : VAL1));

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SERVER_NODES);

        client = true;

        for (int i = SERVER_NODES; i < TOTAL_NODES; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Check topology version.
     *
     * @throws Exception If failed.
     */
    public void testTopologyVersion() throws Exception {
        assertEquals(grid(0).cluster().topologyVersion(), attr("TopologyVersion"));
    }

    /**
     * Check count of client nodes.
     *
     * @throws Exception If failed.
     */
    public void testClientsCount() throws Exception {
        assertEquals(CLIENT_NODES, attr("TotalClientNodes"));
    }

    /**
     * Check count of server nodes.
     *
     * @throws Exception If failed.
     */
    public void testServersCount() throws Exception {
        assertEquals(SERVER_NODES, attr("TotalServerNodes"));
    }

    /**
     * Check nodes count filter.
     *
     * @throws Exception If failed.
     */
    public void testNodesFilter() throws Exception {
        assertEquals(TOTAL_NODES,
            countNodes(IgniteNodeAttributes.ATTR_BUILD_VER, IgniteVersionUtils.VER_STR, true, true));

        assertEquals(SERVER_NODES, countNodes(IgniteNodeAttributes.ATTR_CLIENT_MODE, "false", true, true));
        assertEquals(CLIENT_NODES, countNodes(IgniteNodeAttributes.ATTR_CLIENT_MODE, "true", true, true));

        assertEquals(1, countNodes(ATTR0, VAL0, true, true));
        assertEquals(2, countNodes(ATTR0, VAL1, true, true));
        assertEquals(1, countNodes(ATTR0, VAL1, true, false));
        assertEquals(1, countNodes(ATTR0, VAL1, false, true));
        assertEquals(0, countNodes(ATTR0, VAL1, false, false));

        assertEquals(1, countNodes(ATTR1, VAL0, true, true));
        assertEquals(1, countNodes(ATTR1, VAL0, true, false));
        assertEquals(0, countNodes(ATTR1, VAL0, false, true));
        assertEquals(1, countNodes(ATTR1, VAL1, true, true));
        assertEquals(0, countNodes(ATTR1, VAL1, true, false));
        assertEquals(1, countNodes(ATTR1, VAL1, false, true));
    }

    /**
     * Check the number of nodes grouped by attribute value.
     *
     * @throws Exception If failed.
     */
    public void testNodesGroup() throws Exception {
        assertEquals(Collections.emptyMap(), groupNodes(ATTR0, false, false));

        assertEquals(F.asMap(IgniteVersionUtils.VER_STR, TOTAL_NODES),
            groupNodes(IgniteNodeAttributes.ATTR_BUILD_VER, true, true));

        assertEquals(F.asMap(false, SERVER_NODES, true, CLIENT_NODES),
            groupNodes(IgniteNodeAttributes.ATTR_CLIENT_MODE, true, true));

        assertEquals(F.asMap(VAL0, 1, VAL1, 2), groupNodes(ATTR0, true, true));
        assertEquals(F.asMap(VAL1, 1), groupNodes(ATTR0, false, true));
        assertEquals(F.asMap(VAL0, 1, VAL1, 1), groupNodes(ATTR0, true, false));

        assertEquals(F.asMap(VAL0, 1, VAL1, 1), groupNodes(ATTR1, true, true));
        assertEquals(F.asMap(VAL0, 1), groupNodes(ATTR1, true, false));
        assertEquals(F.asMap(VAL1, 1), groupNodes(ATTR1, false, true));
    }

    /**
     * Invoke "countNodes" method from MXBean.
     *
     * @param name Node attribute name,
     * @param val Node attribute value,
     * @param srv Include server nodes.
     * @param client Include client nodes.
     * @return Count of nodes filtered by node attribute.
     * @throws Exception If failed.
     */
    private int countNodes(String name, String val, boolean srv, boolean client) throws Exception {
        String[] signature = {"java.lang.String", "java.lang.String", "boolean", "boolean"};
        Object[] params = {name, val, srv, client};

        MBeanServer mxbSrv = ManagementFactory.getPlatformMBeanServer();

        return (int)mxbSrv.invoke(mxbeanName(), "countNodes", params, signature);
    }

    /**
     * Invoke "groupNodes" method from MXBean.
     *
     * @param name Node attribute name.
     * @param srv Include server nodes.
     * @param client Include client nodes.
     * @return The number of nodes grouped by node attribute name.
     * @throws Exception If failed.
     */
    private Map groupNodes(String name, boolean srv, boolean client) throws Exception {
        String[] signature = {"java.lang.String", "boolean", "boolean"};
        Object[] params = {name, srv, client};

        MBeanServer mxbSrv = ManagementFactory.getPlatformMBeanServer();

        return (Map)mxbSrv.invoke(mxbeanName(), "groupNodes", params, signature);
    }

    /**
     * Get attribute of MXBean.
     *
     * @param name MXBean attribute name.
     * @return Current value of attribute.
     * @throws Exception If failed.
     */
    private Object attr(String name) throws Exception {
        MBeanServer mxbSrv = ManagementFactory.getPlatformMBeanServer();

        return mxbSrv.getAttribute(mxbeanName(), name);
    }

    /**
     * Make qualified MXBean object name.
     *
     * @return MXBean name.
     * @throws MalformedObjectNameException Thrown in case of any errors.
     */
    private ObjectName mxbeanName() throws MalformedObjectNameException {
        String instanceName = grid(0).configuration().getIgniteInstanceName();

        return U.makeMBeanName(instanceName, "Kernal", IgniteTopologyMXBeanImpl.class.getSimpleName());
    }
}
