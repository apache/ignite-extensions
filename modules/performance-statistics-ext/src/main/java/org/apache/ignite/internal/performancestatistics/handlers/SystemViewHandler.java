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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Builds JSON with system view tables.
 *
 * Example:
 * <pre>
 * {
 *    $nodeId: {
 *      $systemViewTable: [
 *        { $column1: $value1, $column2: $value2 },
 *        { $column1: $value3, $column2: $value4 }
 *        ]
 *    }
 * }
 * </pre>
 */
public class SystemViewHandler implements IgnitePerformanceStatisticsHandler {
    /** System view tables. */
    private final Map<UUID, Map<String, List<Map<String, Object>>>> results = new HashMap<>();

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode objNode = MAPPER.createObjectNode();

        results.forEach((id, view) -> {
            ObjectNode gridObjNode = MAPPER.createObjectNode();

            view.forEach((viewName, table) -> {

                ArrayNode viewTableArrNode = generateTableNode(table);

                gridObjNode.set(viewName, viewTableArrNode);
            });

            objNode.set(id.toString(), gridObjNode);
        });

        return Map.of("systemView", objNode);
    }

    private static ArrayNode generateTableNode(List<Map<String, Object>> table) {
        ArrayNode viewTableArrNode = MAPPER.createArrayNode();

        for (Map<String, Object> row : table) {

            ObjectNode rowNode = MAPPER.createObjectNode();

            for (Map.Entry<String, Object> entry : row.entrySet())
                rowNode.put(entry.getKey(), String.valueOf(entry.getValue()));

            viewTableArrNode.add(rowNode);
        }
        return viewTableArrNode;
    }

    /** {@inheritDoc} */
    @Override public void systemView(UUID nodeId, String viewName, List<String> schema, List<Object> data) {
        Map<String, List<Map<String, Object>>> nodeData = results.computeIfAbsent(nodeId, uuid -> new HashMap<>());

        List<Map<String, Object>> viewData = nodeData.computeIfAbsent(viewName, string -> new ArrayList<>());

        Map <String, Object> row = new TreeMap<>();

        for (int i = 0; i < data.size(); i++) {
            row.put(schema.get(i), data.get(i));
        }

        viewData.add(row);
    }
}
