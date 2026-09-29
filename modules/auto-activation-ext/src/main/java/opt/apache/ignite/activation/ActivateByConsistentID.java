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

package opt.apache.ignite.activation;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Activate cluster when nodes with specified ConsistentID values join topology.
 */
public class ActivateByConsistentID implements IgnitePredicate<Ignite> {
    /** Collection of required nodes ConsistentIDs. */
    private final Set<String> requiredNodes;

    /**
     * @param requiredNodes List of ConsistentIDs.
     */
    public ActivateByConsistentID(Set<String> requiredNodes) {
        if (requiredNodes == null || requiredNodes.isEmpty())
            throw new IllegalArgumentException("requiredNodes must be set");

        this.requiredNodes = requiredNodes;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Ignite grid) {
        Set<String> missingNodes = new HashSet<>(requiredNodes);
        ClusterGroup servers = grid.cluster().forServers();
        IgniteMessaging messaging = grid.message(servers);

        for (ClusterNode node : servers.nodes()) {
            String nodeConsistentId = node.consistentId().toString();

            missingNodes.remove(nodeConsistentId);

            if (missingNodes.isEmpty()) {
                messaging.send(
                    "auto-activation-plugin-events",
                    "Auto activation plugin set cluster state ACTIVE - activation condition meet (by consistent ID)"
                );

                return true;
            }
        }

        messaging.send(
            "auto-activation-plugin-events",
            "Auto activation skipped - activation condition not meet (by consistent ID). Missing nodes " +
                        "[" + String.join(", ", missingNodes) + "]");

        return false;
    }
}
