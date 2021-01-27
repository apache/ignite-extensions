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
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsHandler;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static com.fasterxml.jackson.core.io.CharTypes.appendQuoted;
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

    /** */
    StringBuilder sb = new StringBuilder();

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

        sb.setLength(0);

        sb.append("{\"op\":\"").append(CACHE_START).append("\",");
        sb.append("\"nodeId\":\"").append(nodeId).append("\",");
        sb.append("\"cacheId\":").append(cacheId).append(",");
        sb.append("\"name\":\"");
        appendQuoted(sb, name);
        sb.append("\"}");

        ps.println(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime, long duration) {
        if (skip(type, startTime, cacheId))
            return;

        sb.setLength(0);

        sb.append("{\"op\":\"").append(type).append("\",");
        sb.append("\"nodeId\":\"").append(nodeId).append("\",");
        sb.append("\"cacheId\":").append(cacheId).append(",");
        sb.append("\"startTime\":").append(startTime).append(",");
        sb.append("\"duration\":").append(duration).append("}");

        ps.println(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration,
        boolean commited) {
        OperationType op = commited ? TX_COMMIT : TX_ROLLBACK;

        if (skip(op, startTime, cacheIds))
            return;

        sb.setLength(0);

        sb.append("{\"op\":\"").append(op).append("\",");
        sb.append("\"nodeId\":\"").append(nodeId).append("\",");
        sb.append("\"cacheIds\":").append(cacheIds).append(",");
        sb.append("\"startTime\":").append(startTime).append(",");
        sb.append("\"duration\":").append(duration).append("}");

        ps.println(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
        long duration, boolean success) {
        if (skip(QUERY, startTime))
            return;

        sb.setLength(0);

        sb.append("{\"op\":\"").append(QUERY).append("\",");
        sb.append("\"nodeId\":\"").append(nodeId).append("\",");
        sb.append("\"type\":\"").append(type).append("\",");
        sb.append("\"text\":\"");
        appendQuoted(sb, text);
        sb.append("\",\"id\":").append(id).append(",");
        sb.append("\"startTime\":").append(startTime).append(",");
        sb.append("\"duration\":").append(duration).append(",");
        sb.append("\"success\":").append(success).append("}");

        ps.println(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
        long logicalReads, long physicalReads) {
        if (skip(QUERY_READS))
            return;

        sb.setLength(0);

        sb.append("{\"op\":\"").append(QUERY_READS).append("\",");
        sb.append("\"nodeId\":\"").append(nodeId).append("\",");
        sb.append("\"type\":\"").append(type).append("\",");
        sb.append("\"queryNodeId\":\"").append(queryNodeId).append("\",");
        sb.append("\"id\":").append(id).append(",");
        sb.append("\"logicalReads\":").append(logicalReads).append(",");
        sb.append("\"physicalReads\":").append(physicalReads).append("}");

        ps.println(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
        int affPartId) {
        if (skip(TASK, startTime))
            return;

        sb.setLength(0);

        sb.append("{\"op\":\"").append(TASK).append("\",");
        sb.append("\"nodeId\":\"").append(nodeId).append("\",");
        sb.append("\"sesId\":\"").append(sesId).append("\",");
        sb.append("\"taskName\":\"");
        appendQuoted(sb, taskName);
        sb.append("\",\"startTime\":").append(startTime).append(",");
        sb.append("\"duration\":").append(duration).append(",");
        sb.append("\"affPartId\":").append(affPartId).append("}");

        ps.println(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration,
        boolean timedOut) {
        if (skip(JOB, startTime))
            return;

        sb.setLength(0);

        sb.append("{\"op\":\"").append(JOB).append("\",");
        sb.append("\"nodeId\":\"").append(nodeId).append("\",");
        sb.append("\"sesId\":\"").append(sesId).append("\",");
        sb.append("\"queuedTime\":").append(queuedTime).append(",");
        sb.append("\"startTime\":").append(startTime).append(",");
        sb.append("\"duration\":").append(duration).append(",");
        sb.append("\"timedOut\":").append(timedOut).append("}");

        ps.println(sb.toString());
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
        return skip(op) || !(cacheIds == null || cacheIds.contains(cacheId));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime, int cacheId) {
        return skip(op, startTime) || !(cacheIds == null || cacheIds.contains(cacheId));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime, GridIntList cacheIds) {
        if (skip(op, startTime))
            return true;

        if (this.cacheIds == null)
            return false;

        GridIntIterator iter = cacheIds.iterator();

        while (iter.hasNext()) {
            if (this.cacheIds.contains(iter.next()))
                return false;
        }

        return true;
    }
}
