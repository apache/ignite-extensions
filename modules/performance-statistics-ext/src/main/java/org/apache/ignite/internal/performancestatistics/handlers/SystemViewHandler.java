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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Writes system view tables into chunked JSON files.
 */
public class SystemViewHandler implements IgnitePerformanceStatisticsHandler {
    /** Number of rows per chunk. */
    private static final int CHUNK_SIZE = 2_000;

    /** Base directory for system view data. */
    private final Path baseDir;

    /** Per-view state in traversal order. */
    private final Map<String, ViewState> views = new HashMap<>();

    /** Current view being written. */
    private ViewState currentView;

    /** All node IDs seen in system views. */
    private final Set<String> nodeIds = new HashSet<>();

    /** */
    public SystemViewHandler(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    /** {@inheritDoc} */
    @Override public void systemView(UUID nodeId, String viewName, List<String> schema, List<Object> data) {
        String nodeIdStr = nodeId.toString();

        nodeIds.add(nodeIdStr);

        try {
            if (currentView == null || !currentView.name.equals(viewName)) {
                if (currentView != null)
                    currentView.finishCurrentNode();

                currentView = views.get(viewName);

                if (currentView == null) {
                    currentView = new ViewState(viewName, schema);
                    views.put(viewName, currentView);
                }
            }

            currentView.addRow(nodeIdStr, data);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        try {
            for (ViewState view : views.values())
                view.finish();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        ObjectNode meta = MAPPER.createObjectNode();

        ArrayNode viewsNode = meta.putArray("views");
        views.values().forEach(state -> viewsNode.add(state.name));

        ArrayNode nodesNode = meta.putArray("nodes");
        nodeIds.forEach(nodesNode::add);

        return Collections.singletonMap("systemViewMeta", meta);
    }

    /** Holds state for a single view of every node. */
    private class ViewState {
        /** */
        private final String name;

        /** */
        private final List<String> schema;

        /** */
        private final Path viewDir;

        /** */
        private final Map<String, Integer> nodeRowCounts = new TreeMap<>();

        /** */
        private String currentNodeId;

        /** */
        private NodeState currentNode;

        /** */
        private ViewState(String viewName, List<String> schema) throws IOException {
            this.name = viewName;
            this.schema = schema;
            this.viewDir = baseDir.resolve(viewName);

            Files.createDirectories(viewDir);
        }

        /** */
        public void addRow(String nodeId, List<Object> data) throws IOException {
            if (currentNode == null || !nodeId.equals(currentNodeId)) {
                finishCurrentNode();
                currentNodeId = nodeId;
                currentNode = new NodeState(viewDir.resolve(nodeId));
            }

            currentNode.addRow(data);
        }

        /** */
        public void finish() throws IOException {
            finishCurrentNode();

            ObjectNode meta = MAPPER.createObjectNode();
            meta.put("chunkSize", CHUNK_SIZE);

            ArrayNode columnsNode = meta.putArray("columns");
            schema.forEach(columnsNode::add);

            ObjectNode nodesNode = meta.putObject("nodes");
            nodeRowCounts.forEach(nodesNode::put);

            Path metaFile = viewDir.resolve("meta.json");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(new File(metaFile.toString()), meta);
        }

        /** */
        private void finishCurrentNode() throws IOException {
            if (currentNode == null)
                return;

            currentNode.closeChunk();

            nodeRowCounts.put(currentNodeId, currentNode.rowCount);

            currentNode = null;
            currentNodeId = null;
        }
    }

    /** */
    private static class NodeState {
        /** */
        private final Path nodeDir;

        /** */
        private int chunkIdx;

        /** */
        private int chunkRowCount;

        /** */
        private int rowCount;

        /** */
        private JsonGenerator generator;

        /** */
        private NodeState(Path nodeDir) throws IOException {
            this.nodeDir = nodeDir;
            Files.createDirectories(nodeDir);
        }

        /** */
        public void addRow(List<Object> data) {
            try {
                ensureChunk();

                generator.writeStartArray();
                for (Object attr : data)
                    generator.writeString(String.valueOf(attr));
                generator.writeEndArray();

                chunkRowCount++;
                rowCount++;

                if (chunkRowCount >= CHUNK_SIZE)
                    closeChunk();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /** */
        public void ensureChunk() throws IOException {
            if (generator != null)
                return;

            Path chunkFile = nodeDir.resolve(String.format("chunk-%06d.json", chunkIdx));

            generator = MAPPER.getFactory().createGenerator(Files.newBufferedWriter(chunkFile));

            generator.writeStartObject();
            generator.writeArrayFieldStart("rows");
        }

        /** */
        public void closeChunk() throws IOException {
            if (generator == null)
                return;

            generator.writeEndArray();
            generator.writeEndObject();
            generator.close();

            generator = null;
            chunkRowCount = 0;
            chunkIdx++;
        }
    }
}
