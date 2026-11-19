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

package org.apache.ignite.activation;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteClosure;

/** Auto activation condition: all servers with the given consistent IDs are in the topology. */
public class ActivateByConsistentID implements IgniteClosure<ClusterGroup, Map.Entry<Boolean, String>> {
    /** Collection of required nodes ConsistentIDs. */
    private final Set<String> requiredNodes;

    /** @param requiredNodes List of ConsistentIDs. */
    public ActivateByConsistentID(Set<String> requiredNodes) {
        if (requiredNodes == null || requiredNodes.isEmpty())
            throw new IllegalArgumentException("requiredNodes must be set");

        this.requiredNodes = requiredNodes;
    }

    /** {@inheritDoc} */
    @Override public Map.Entry<Boolean, String> apply(ClusterGroup servers) {
        Set<String> missingNodes = new HashSet<>(requiredNodes);

        for (ClusterNode node : servers.nodes()) {
            String nodeConsistentId = node.consistentId().toString();

            missingNodes.remove(nodeConsistentId);

            if (missingNodes.isEmpty()) {
                return Map.entry(true, "Auto activation plugin set cluster state ACTIVE - " +
                        "activation condition meet (by consistent ID)");
            }
        }

        return Map.entry(false, "Auto activation skipped - activation condition not met (by consistent ID). " +
                "Missing nodes " + "[" + String.join(", ", missingNodes) + "]");
    }
}
