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
 * Activate cluster when nodes with all specified attributes values join topology.
 */
public class ActivateByNodeAttribute implements IgnitePredicate<Ignite> {
    /** Node's attribute name. */
    private final String attrName;

    /** Collection of values for node's attribute. */
    private final Set<String> requiredValues;

    /**
     * @param attributeName Node's attribute name.
     * @param requiredValues List of values for node's attribute.
     */
    public ActivateByNodeAttribute(String attributeName, Set<String> requiredValues) {
        if (attributeName == null || attributeName.isBlank())
            throw new IllegalArgumentException("attributeName must be set");

        if (requiredValues == null || requiredValues.isEmpty())
            throw new IllegalArgumentException("requiredValues must be set");

        this.attrName = attributeName;
        this.requiredValues = requiredValues;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Ignite grid) {
        Set<String> missingValues = new HashSet<>(requiredValues);

        ClusterGroup servers = grid.cluster().forServers();

        IgniteMessaging messaging = grid.message(servers);

        for (ClusterNode node : servers.nodes()) {
            String attrVal = node.attribute(attrName);

            missingValues.remove(attrVal);

            if (missingValues.isEmpty()) {
                messaging.send(
                    "auto-activation-plugin-events",
                    "Auto activation plugin set cluster state ACTIVE - activation condition meet (by node attribute)"
                );

                return true;
            }
        }

        messaging.send("auto-activation-plugin-events",
            "Auto activation skipped - activation condition not meet (by node attribute). "
                    + "Attribute: " + attrName + ", Missing values [" + String.join(", ", missingValues) + "]");

        return false;
    }
}
