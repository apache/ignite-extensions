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

import java.io.PrintStream;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsHandler;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;

/**
 * Handler to print performance statistics operations.
 */
public class PrintHandler implements PerformanceStatisticsHandler {
    /** Print stream. */
    private final PrintStream ps;

    /** Json mapper. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** @param ps Print stream. */
    public PrintHandler(PrintStream ps) {
        this.ps = ps;
    }

    /** {@inheritDoc} */
    @Override public void cacheStart(UUID nodeId, int cacheId, String name) {
        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", CACHE_START.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("cacheId", cacheId);
        json.put("name", name);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
        long duration) {
        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", String.valueOf(type));
        json.put("nodeId", String.valueOf(nodeId));
        json.put("cacheId", cacheId);
        json.put("startTime", startTime);
        json.put("duration", duration);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration,
        boolean commited) {
        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", commited ? TX_COMMIT.toString() : TX_ROLLBACK.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("cacheIds", String.valueOf(cacheIds));
        json.put("startTime", startTime);
        json.put("duration", duration);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
        long duration, boolean success) {
        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", QUERY.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("type", String.valueOf(type));
        json.put("text", text);
        json.put("id", id);
        json.put("startTime", startTime);
        json.put("duration", duration);
        json.put("success", success);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
        long logicalReads, long physicalReads) {
        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", QUERY_READS.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("type", String.valueOf(type));
        json.put("queryNodeId", String.valueOf(queryNodeId));
        json.put("id", id);
        json.put("logicalReads", logicalReads);
        json.put("physicalReads", physicalReads);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
        int affPartId) {
        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", TASK.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("sesId", String.valueOf(sesId));
        json.put("taskName", taskName);
        json.put("startTime", startTime);
        json.put("duration", duration);
        json.put("affPartId", affPartId);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration,
        boolean timedOut) {
        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", JOB.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("sesId", String.valueOf(sesId));
        json.put("queuedTime", queuedTime);
        json.put("startTime", startTime);
        json.put("duration", duration);
        json.put("timedOut", timedOut);

        ps.println(json.toString());
    }
}
