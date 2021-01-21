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
import java.util.BitSet;
import java.util.Set;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsHandler;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

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
    /** Json mapper. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Print stream. */
    private final PrintStream ps;

    /** Operation types. */
    @Nullable private final BitSet ops;

    /** Start time from. */
    private final long from;

    /** Start time to. */
    private final long to;

    /** Cache identifiers to filter the output. */
    @Nullable private final Set<Integer> cacheIds;

    /**
     * @param ps Print stream.
     * @param ops Set of operations to print.
     * @param from The minimum operation start time to filter the output.
     * @param to The maximum operation start time to filter the output.
     * @param cacheIds Cache identifiers to filter the output.
     */
    public PrintHandler(PrintStream ps, @Nullable BitSet ops, long from, long to, @Nullable Set<Integer> cacheIds) {
        this.ps = ps;
        this.ops = ops;
        this.from = from;
        this.to = to;
        this.cacheIds = cacheIds;
    }

    /** {@inheritDoc} */
    @Override public void cacheStart(UUID nodeId, int cacheId, String name) {
        if (skip(CACHE_START, cacheId))
            return;

        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", CACHE_START.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("cacheId", cacheId);
        json.put("name", name);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime, long duration) {
        if (skip(type, startTime, cacheId))
            return;

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
        OperationType op = commited ? TX_COMMIT : TX_ROLLBACK;

        if (skip(op, startTime, cacheIds))
            return;

        ObjectNode json = MAPPER.createObjectNode();

        json.put("op", op.toString());
        json.put("nodeId", String.valueOf(nodeId));
        json.put("cacheIds", String.valueOf(cacheIds));
        json.put("startTime", startTime);
        json.put("duration", duration);

        ps.println(json.toString());
    }

    /** {@inheritDoc} */
    @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
        long duration, boolean success) {
        if (skip(QUERY, startTime))
            return;

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
        if (skip(QUERY_READS))
            return;

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
        if (skip(TASK, startTime))
            return;

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
        if (skip(JOB, startTime))
            return;

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

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op) {
        return !(ops == null || ops.get(op.id()));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime) {
        return skip(op) || startTime < from || startTime > to;
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, int cacheId) {
        return skip(op) || (cacheIds != null && cacheIds.contains(cacheId));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime, int cacheId) {
        return skip(op, startTime) || (cacheIds != null && cacheIds.contains(cacheId));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime, GridIntList cacheIds) {
        if (skip(op, startTime))
            return true;

        if (cacheIds == null)
            return false;

        GridIntIterator iter = cacheIds.iterator();

        while (iter.hasNext()) {
            if (cacheIds.contains(iter.next()))
                return false;
        }

        return true;
    }
}
