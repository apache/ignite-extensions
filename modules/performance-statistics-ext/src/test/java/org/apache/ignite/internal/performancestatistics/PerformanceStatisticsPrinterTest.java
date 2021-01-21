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

package org.apache.ignite.internal.performancestatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the performance statistics printer.
 */
public class PerformanceStatisticsPrinterTest {
    /** Test output file name. */
    private final static String OUTPUT_FILE_NAME = "report.txt";

    /** */
    private final static List<OperationType> EXP_OPS = F.asList(CACHE_START, CACHE_GET, CACHE_PUT,
        TX_COMMIT, TX_ROLLBACK, QUERY, QUERY_READS, TASK, JOB);

    /** Operations with start time. */
    private final static List<OperationType> OPS_WITH_START_TIME = F.asList(CACHE_GET, CACHE_PUT,
        TX_COMMIT, TX_ROLLBACK, QUERY, TASK, JOB);

    /** Test node ID. */
    private final static UUID NODE_ID = UUID.randomUUID();

    /** */
    private final static long START_TIME_1 = 10;

    /** */
    private final static long START_TIME_2 = 20;

    /** */
    private final static int CACHE_ID_1 = 1;

    /** */
    private final static int CACHE_ID_2 = 2;

    /** Cache 1 operations. */
    private final static List<OperationType> CACHE_1_OPS = F.asList(CACHE_GET, TX_ROLLBACK);

    /** Cache 2 operations. */
    private final static List<OperationType> CACHE_2_OPS = F.asList(CACHE_PUT, TX_COMMIT);


    /** Performance statistics files directory. */
    private static File perfStatDir;

    /** */
    @BeforeClass
    public static void beforeTests() throws Exception {
        FilePerformanceStatisticsWriter writer = new FilePerformanceStatisticsWriter(new TestKernalContext(NODE_ID));

        writer.start();

        for (int cacheId : new int[] {CACHE_ID_1, CACHE_ID_2})
            writer.cacheStart(cacheId, "cache-" + cacheId);

        for (long startTime : new long[] {START_TIME_1, START_TIME_2}) {
            for (int cacheId : new int[] {CACHE_ID_1, CACHE_ID_2}) {
                writer.cacheOperation(CACHE_GET, cacheId, startTime, 0);
                writer.cacheOperation(CACHE_PUT, cacheId, startTime, 0);
                writer.transaction(GridIntList.asList(cacheId), startTime, 0, true);
                writer.transaction(GridIntList.asList(cacheId), startTime, 0, false);
            }

            writer.query(GridCacheQueryType.SQL_FIELDS, "query", 0, startTime, 0, true);
            writer.queryReads(GridCacheQueryType.SQL_FIELDS, NODE_ID, 0, 0, 0);
            writer.task(new IgniteUuid(NODE_ID, 0), "", startTime, 0, 0);
            writer.job(new IgniteUuid(NODE_ID, 0), 0, startTime, 0, true);
        }

        writer.stop();

        perfStatDir = new File(U.defaultWorkDirectory(), PERF_STAT_DIR);

        assertTrue(perfStatDir.exists());
    }

    /** */
    @AfterClass
    public static void afterTests() throws Exception {
        U.delete(new File(U.defaultWorkDirectory()));
    }

    /** @throws Exception If failed. */
    @Test
    public void testOperationsFilter() throws Exception {
        checkOperationFilter(null, EXP_OPS);

        checkOperationFilter(F.asList(CACHE_START), F.asList(CACHE_START));

        checkOperationFilter(F.asList(TASK, JOB), F.asList(TASK, JOB));

        checkOperationFilter(OPS_WITH_START_TIME, OPS_WITH_START_TIME);

        checkOperationFilter(EXP_OPS, EXP_OPS);
    }

    /** */
    private void checkOperationFilter(List<OperationType> opsParam, List<OperationType> expOps) throws Exception {
        List<String> args = new LinkedList<>();

        if (opsParam != null) {
            args.add("--ops");
            args.add(opsParam.stream().map(Enum::toString).collect(joining(",")));
        }

        List<OperationType> ops = new LinkedList<>();

        ops.addAll(expOps);
        ops.addAll(expOps);

        readStatistics(args, json -> {
            OperationType op = OperationType.valueOf(json.get("op").asText());

            assertTrue("Unexpected operation: " + op, ops.remove(op));

            UUID nodeId = UUID.fromString(json.get("nodeId").asText());

            assertEquals(NODE_ID, nodeId);
        });

        assertTrue("Expected operations:" + ops, ops.isEmpty());
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartTimeFilter() throws Exception {
        checkStartTimeFilter(null, null, F.asList(START_TIME_1, START_TIME_2));

        checkStartTimeFilter(null, START_TIME_1, F.asList(START_TIME_1));

        checkStartTimeFilter(START_TIME_2, null, F.asList(START_TIME_2));

        checkStartTimeFilter(START_TIME_1, START_TIME_2, F.asList(START_TIME_1, START_TIME_2));
    }

    /** */
    private void checkStartTimeFilter(Long from, Long to, List<Long> expTimes) throws Exception  {
        List<String> args = new LinkedList<>();

        if (from != null) {
            args.add("--from");
            args.add(from.toString());
        }

        if (to != null) {
            args.add("--to");
            args.add(to.toString());
        }

        List<OperationType> ops = new LinkedList<>();
        List<Long> times = new LinkedList<>();

        for (Long time : expTimes) {
            ops.addAll(OPS_WITH_START_TIME);

            for (OperationType op : OPS_WITH_START_TIME)
                times.add(time);
        }

        readStatistics(args, json -> {
            OperationType op = OperationType.valueOf(json.get("op").asText());

            if (OPS_WITH_START_TIME.contains(op)) {
                assertTrue("Unexpected operation: " + op, ops.remove(op));

                long startTime = json.get("startTime").asLong();

                assertTrue("Unexpected startTime: " + startTime, times.remove(startTime));
            }
        });

        assertTrue("Expected operations:" + ops, ops.isEmpty());
        assertTrue("Expected times:" + ops, times.isEmpty());
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheIdsFilter() throws Exception {
        checkCacheIdsFilter(null, new int[] {CACHE_ID_1, CACHE_ID_2});

        checkCacheIdsFilter(new int[] {CACHE_ID_1}, new int[] {CACHE_ID_1});

        checkCacheIdsFilter(new int[] {CACHE_ID_2}, new int[] {CACHE_ID_2});

        checkCacheIdsFilter(new int[] {CACHE_ID_1, CACHE_ID_2}, new int[] {CACHE_ID_1, CACHE_ID_2});
    }

    /** */
    private void checkCacheIdsFilter(int[] cacheIds, int[] expCacheIds) throws Exception  {
        List<String> args = new LinkedList<>();

        if (cacheIds != null) {
            args.add("--cache-ids");
            args.add(Arrays.stream(cacheIds).mapToObj(String::valueOf).collect(joining(",")));
        }

        List<OperationType> ops = new LinkedList<>();
        List<Integer> ids = new LinkedList<>();

        for (Integer id : expCacheIds) {
            ops.addAll(OPS_WITH_CACHE_ID);

            ids.add(id);
        }

        readStatistics(args, json -> {
            OperationType op = OperationType.valueOf(json.get("op").asText());

            if (OperationType.cacheOperation(op)) {
                assertTrue("Unexpected operation: " + op, ops.remove(op));

                Integer id = json.get("cacheId").asInt();

                assertTrue("Unexpected cache id: " + id, ids.remove(id));
            }
            else if (OperationType.transactionOperation(op)) {
                assertTrue("Unexpected operation: " + op, ops.remove(op));

                Integer id = Integer.valueOf(json.get("cacheIds").asText().replace("[", "").replace("]", ""));

                assertTrue("Unexpected cache id: " + id, ids.remove(id));
            }
        });

        assertTrue("Expected operations:" + ops, ops.isEmpty());
        assertTrue("Expected cache ids:" + ops, ids.isEmpty());
    }

    /**
     * @param args Additional program arguments.
     * @param consumer Consumer to handle operations.
     * @throws Exception If failed.
     */
    private void readStatistics(List<String> args, Consumer<JsonNode> consumer) throws Exception {
        File out = new File(U.defaultWorkDirectory(), OUTPUT_FILE_NAME);

        U.delete(out);

        List<String> pArgs = new LinkedList<>();

        pArgs.add(perfStatDir.getAbsolutePath());
        pArgs.add("--out");
        pArgs.add(out.getAbsolutePath());

        pArgs.addAll(args);

        PerformanceStatisticsPrinter.main(pArgs.toArray(new String[0]));

        assertTrue(out.exists());

        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(out))) {
            String line;

            while ((line = reader.readLine()) != null) {
                JsonNode json = mapper.readTree(line);

                assertTrue(json.isObject());

                UUID nodeId = UUID.fromString(json.get("nodeId").asText());

                assertEquals(NODE_ID, nodeId);

                consumer.accept(json);
            }
        }
    }

    /** Test kernal context. */
    private static class TestKernalContext extends GridTestKernalContext {
        /** Node ID. */
        private final UUID nodeId;

        /** @param nodeId Node ID. */
        public TestKernalContext(UUID nodeId) {
            super(new GridTestLog4jLogger());

            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public UUID localNodeId() {
            return nodeId;
        }
    }
}
