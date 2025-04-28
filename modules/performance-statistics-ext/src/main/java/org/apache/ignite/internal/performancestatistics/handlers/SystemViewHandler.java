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

package org.apache.ignite.internal.performancestatistics.handlers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Builds JSON with system view tables.
 * <p>
 * Example:
 * <pre>
 * {
 *    $nodeId: {
 *      $systemViewTable:
 *        schema: [ $column1, $column2 ],
 *        data:[
 *          [ $value1, $value2 ],
 *          [ $value3, $value4 ]
 *        ]
 *    }
 * }
 * </pre>
 */
public class SystemViewHandler implements IgnitePerformanceStatisticsHandler {
    /** */
    private final ObjectNode resNode = MAPPER.createObjectNode();

    /** {@inheritDoc} */
    @Override public void systemView(UUID nodeId, String viewName, List<String> schema, List<Object> data) {
        JsonNode gridNode = resNode.get(nodeId.toString());

        if (gridNode == null) {
            gridNode = MAPPER.createObjectNode();
            resNode.set(nodeId.toString(), gridNode);
        }

        JsonNode viewNode = gridNode.get(viewName);

        if (viewNode == null) {
            viewNode = MAPPER.createObjectNode();
            ((ObjectNode)gridNode).set(viewName, viewNode);

            ArrayNode schemaNode = MAPPER.createArrayNode();
            schema.forEach(schemaNode::add);
            ((ObjectNode)viewNode).set("schema", schemaNode);

            ArrayNode dataNode = MAPPER.createArrayNode();
            ((ObjectNode)viewNode).set("data", dataNode);
        }

        ArrayNode dataNode = (ArrayNode)viewNode.get("data");

        ArrayNode rowNode = MAPPER.createArrayNode();
        data.forEach(attr -> rowNode.add(String.valueOf(attr)));
        dataNode.add(rowNode);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        return Collections.singletonMap("systemView", resNode);
    }
}
