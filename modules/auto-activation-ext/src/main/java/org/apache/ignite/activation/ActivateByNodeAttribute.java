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
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteClosure;

/** Auto activation condition: all servers with the given attributes values are in the topology. */
public class ActivateByNodeAttribute implements IgniteClosure<ClusterGroup, Map.Entry<Boolean, String>> {
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
    @Override public Map.Entry<Boolean, String> apply(ClusterGroup servers) {
        Set<String> missingValues = new HashSet<>(requiredValues);

        for (ClusterNode node : servers.nodes()) {
            String attrVal = Objects.toString(node.attribute(attrName), null);

            missingValues.remove(attrVal);

            if (missingValues.isEmpty())
                return Map.entry(true, "Auto activation plugin set cluster state ACTIVE - activation " +
                        "condition meet (by node attribute)");
        }

        return Map.entry(false, "Auto activation skipped - activation condition not met (by node attribute). "
                + "Attribute: " + attrName + ", Missing values [" + String.join(", ", missingValues) + "]");
    }
}
