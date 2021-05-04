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

import java.io.File;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.internal.performancestatistics.handlers.CheckpointHandler;
import org.apache.ignite.internal.performancestatistics.handlers.IgnitePerformanceStatisticsHandler;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.performancestatistics.PerformanceStatisticsPrinterTest.createStatistics;
import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the performance statistics handlers.
 */
public class PerformanceStatisticsHandlersTest {
    /** */
    @Before
    public void beforeTest() throws Exception {
        U.delete(new File(U.defaultWorkDirectory()));
    }

    /** */
    @After
    public void afterTest() throws Exception {
        U.delete(new File(U.defaultWorkDirectory()));
    }

    /** */
    @Test
    public void testCheckpoints() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long beforeLockDuration = rnd.nextLong();
        long lockWaitDuration = rnd.nextLong();
        long listenersExecDuration = rnd.nextLong();
        long markDuration = rnd.nextLong();
        long lockHoldDuration = rnd.nextLong();
        long pagesWriteDuration = rnd.nextLong();
        long fsyncDuration = rnd.nextLong();
        long walCpRecordFsyncDuration = rnd.nextLong();
        long writeCpEntryDuration = rnd.nextLong();
        long splitAndSortCpPagesDuration = rnd.nextLong();
        long totalDuration = rnd.nextLong();
        long cpStartTime = rnd.nextLong();
        int pagesSize = rnd.nextInt();
        int dataPagesWritten = rnd.nextInt();
        int cowPagesWritten = rnd.nextInt();

        createStatistics(writer -> {
            writer.checkpoint(beforeLockDuration,
                lockWaitDuration,
                listenersExecDuration,
                markDuration,
                lockHoldDuration,
                pagesWriteDuration,
                fsyncDuration,
                walCpRecordFsyncDuration,
                writeCpEntryDuration,
                splitAndSortCpPagesDuration,
                totalDuration,
                cpStartTime,
                pagesSize,
                dataPagesWritten,
                cowPagesWritten);
        });

        readResultStatistics(jsonNode -> {
                JsonNode json = jsonNode.get("checkpointsInfo")
                    .get("checkpoints")
                    .get(0);

                assertEquals(json.get("beforeLockDuration").asLong(), beforeLockDuration);
                assertEquals(json.get("lockWaitDuration").asLong(), lockWaitDuration);
                assertEquals(json.get("listenersExecDuration").asLong(), listenersExecDuration);
                assertEquals(json.get("markDuration").asLong(), markDuration);
                assertEquals(json.get("lockHoldDuration").asLong(), lockHoldDuration);
                assertEquals(json.get("pagesWriteDuration").asLong(), pagesWriteDuration);
                assertEquals(json.get("fsyncDuration").asLong(), fsyncDuration);
                assertEquals(json.get("walCpRecordFsyncDuration").asLong(), walCpRecordFsyncDuration);
                assertEquals(json.get("writeCheckpointEntryDuration").asLong(), writeCpEntryDuration);
                assertEquals(json.get("splitAndSortCpPagesDuration").asLong(), splitAndSortCpPagesDuration);
                assertEquals(json.get("totalDuration").asLong(), totalDuration);
                assertEquals(json.get("cpStartTime").asLong(), cpStartTime);
                assertEquals(json.get("pagesSize").asInt(), pagesSize);
                assertEquals(json.get("dataPagesWritten").asInt(), dataPagesWritten);
                assertEquals(json.get("cowPagesWritten").asInt(), cowPagesWritten);
            },
            new CheckpointHandler());
    }

    /** */
    @Test
    public void testPagesWriteThrottle() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long endTime = System.currentTimeMillis();
        long duration = rnd.nextInt(200);

        createStatistics(writer -> {
            writer.pagesWriteThrottle(endTime, duration);
        });

       readResultStatistics(jsonNode -> {
           JsonNode json = jsonNode.get("checkpointsInfo")
               .get("throttles")
               .get(0);

           assertEquals(1, json.get("counter").asLong());
           assertEquals(duration, json.get("duration").asLong());
           assertEquals(TimeUnit.MILLISECONDS.toSeconds(endTime),
               TimeUnit.MILLISECONDS.toSeconds(json.get("time").asLong()));
       },
           new CheckpointHandler());
    }

    /**
     * @throws Exception If failed.
     */
    private void readResultStatistics(Consumer<JsonNode> c, IgnitePerformanceStatisticsHandler... handlers)
        throws Exception {
        File perfStatDir = new File(U.defaultWorkDirectory(), PERF_STAT_DIR);

        assertTrue(perfStatDir.exists());

        File out = new File(U.defaultWorkDirectory(), "report.txt");

        U.delete(out);

        new FilePerformanceStatisticsReader(handlers).read(Collections.singletonList(perfStatDir));

        ObjectNode dataJson = MAPPER.createObjectNode();

        for (IgnitePerformanceStatisticsHandler handler : handlers)
            handler.results().forEach(dataJson::set);

        JsonNode json = MAPPER.readTree(dataJson.toString());

        c.accept(json);
    }
}
