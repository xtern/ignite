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

package org.apache.ignite.internal.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterNodeFilter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.mxbean.IgniteTopologyMXBean;

/**
 * Management bean for monitoring cluster topology.
 */
public class IgniteTopologyMXBeanImpl implements IgniteTopologyMXBean {
    /** Grid discovery manager. */
    private final GridDiscoveryManager discoMgr;

    /**
     * @param discoMgr {@link GridDiscoveryManager Grid discovery manager}.
     */
    public IgniteTopologyMXBeanImpl(GridDiscoveryManager discoMgr) {
        this.discoMgr = discoMgr;
    }

    /** {@inheritDoc} */
    @Override public int getTotalServerNodes() {
        return discoMgr.discoCache().serverNodes().size();
    }

    /** {@inheritDoc} */
    @Override public int getTotalClientNodes() {
        DiscoCache discoCache = discoMgr.discoCache();

        return discoCache.allNodes().size() - discoCache.serverNodes().size() - discoCache.daemonNodes().size();
    }

    /** {@inheritDoc} */
    @Override public long getTopologyVersion() {
        return discoMgr.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public int countNodes(String name, String val, boolean srv, boolean client) {
        T2<String, String> attr = new T2<>(name, val);
        DiscoCache discoCache = discoMgr.discoCache();
        List<ClusterNode> nodes = discoCache.allNodes();

        if (srv && !client)
            nodes = discoCache.serverNodes();

        return F.size(nodes, new InternalNodeFilter(srv, client, attr));
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Integer> groupNodes(String name, boolean srv, boolean client) {
        DiscoCache discoCache = discoMgr.discoCache();
        List<ClusterNode> nodes = discoCache.allNodes();

        if (srv && !client)
            nodes = discoCache.serverNodes();

        return groupNodesByAttr(nodes, name, srv, client);
    }

    /**
     * Map nodes by attribute value.
     *
     * @param nodes Node list.
     * @param attr Attribute name.
     * @param srv Include server nodes.
     * @param client Include client nodes.
     * @return The number of nodes grouped by the node attribute value.
     */
    private Map<Object, Integer> groupNodesByAttr(List<ClusterNode> nodes, String attr, boolean srv, boolean client) {
        Map<Object, Integer> nodeGroups = new HashMap<>();

        for (ClusterNode node : nodes) {
            if (node.isDaemon() || !((srv && !node.isClient()) || (client && node.isClient())))
                continue;

            Object attrVal = node.attribute(attr);

            if (attrVal != null) {
                Integer cnt = nodeGroups.get(attrVal);

                if (cnt == null)
                    nodeGroups.put(attrVal, 1);
                else
                    nodeGroups.put(attrVal, ++cnt);
            }
        }

        return nodeGroups;
    }

    /** */
    private static class InternalNodeFilter implements PlatformClusterNodeFilter {
        /** */
        private static final long serialVersionUID = 1L;

        /** */
        private final boolean client;

        /** */
        private final boolean srv;

        /** Node attribute. */
        private final T2<String, String> attr;

        /**
         * @param srv Include server nodes.
         * @param client Include client nodes.
         * @param attr Node attribute.
         */
        private InternalNodeFilter(boolean srv, boolean client, T2<String, String> attr) {
            this.srv = srv;
            this.client = client;
            this.attr = attr;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            if (node.isDaemon() || !((srv && !node.isClient()) || (client && node.isClient())))
                return false;

            Object val = node.attribute(attr.getKey());

            return val != null && val.toString().equals(attr.getValue());
        }
    }
}
