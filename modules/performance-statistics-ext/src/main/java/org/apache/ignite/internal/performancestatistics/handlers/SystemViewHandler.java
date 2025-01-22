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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Builds JSON with system view tables.
 *
 * Example:
 * <pre>
 * {
 *    $nodeId: {
 *      $systemViewTable: [
 *        {
 *          $column1: $value1,
 *          $column2: $value2,
 *        },
 *        {
 *          $column1: $value3,
 *          $column2: $value4,
 *        }]
 *    }
 * }
 * </pre>
 */
public class SystemViewHandler implements IgnitePerformanceStatisticsHandler {
    /** System view tables. */
    private final Map<UUID, Map<String, List<Map<String, String>>>> results = new TreeMap<>();

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode objNode = MAPPER.createObjectNode();

        results.forEach((id, view) -> {
            ObjectNode gridObjNode = MAPPER.createObjectNode();

            view.forEach((viewName, table) -> {
                ArrayNode viewTableArrNode = MAPPER.createArrayNode();

                for (Map<String, String> row : table) {
                    ObjectNode rowNode = MAPPER.createObjectNode();
                    for (Map.Entry<String, String> entry : row.entrySet())
                        rowNode.put(entry.getKey(), entry.getValue());
                    viewTableArrNode.add(rowNode);
                }

                gridObjNode.set(viewName, viewTableArrNode);
            });

            objNode.set(id.toString(), gridObjNode);
        });

        return U.map("systemView", objNode);
    }

    /** {@inheritDoc} */
    @Override public void systemView(UUID id, String name, long time, Map<String, String> row) {
        Map<String, List<Map<String, String>>> nodeData = results.computeIfAbsent(id, uuid -> new TreeMap<>());
        List<Map<String, String>> viewData = nodeData.computeIfAbsent(name, string -> new ArrayList<>());
        row.put("time created", String.valueOf(time));
        viewData.add(row);
    }
}
