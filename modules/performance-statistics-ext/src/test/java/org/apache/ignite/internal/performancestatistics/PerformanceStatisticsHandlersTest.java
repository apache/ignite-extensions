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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.internal.performancestatistics.handlers.CheckpointHandler;
import org.apache.ignite.internal.performancestatistics.handlers.IgnitePerformanceStatisticsHandler;
import org.apache.ignite.internal.performancestatistics.handlers.PagesWriteThrottleHandler;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.performancestatistics.PerformanceStatisticsTestUtils.createStatistics;
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

        JsonNode json = readResultStatistics(new CheckpointHandler())
            .get(CheckpointHandler.CHECKPOINTS)
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
    }

    /** */
    @Test
    public void testPagesWriteThrottle() throws Exception {
        long endTime1 = 10000;
        long duration1 = 1500;
        long endTime2 = endTime1 + 1000;
        long duration2 = 100;
        long endTime3 = endTime2 + 500;
        long duration3 = 200;

        createStatistics(writer -> {
            writer.pagesWriteThrottle(endTime1, duration1);
            writer.pagesWriteThrottle(endTime2, duration2);
            writer.pagesWriteThrottle(endTime3, duration3);
        });

        JsonNode node = readResultStatistics(new PagesWriteThrottleHandler())
            .get(PagesWriteThrottleHandler.PAGES_WRITE_THROTTLE);

        assertEquals(3, node.size());

        JsonNode node1 = node.get(0);

        assertEquals(1, node1.get("counter").asLong());
        assertEquals(TimeUnit.MILLISECONDS.toSeconds(endTime1) - TimeUnit.MILLISECONDS.toSeconds(duration1),
            TimeUnit.MILLISECONDS.toSeconds(node1.get("time").asLong()));
        assertEquals(1000, node1.get("duration").asLong());

        JsonNode node2 = node.get(1);

        assertEquals(1, node2.get("counter").asLong());
        assertEquals(endTime1, node2.get("time").asLong());
        assertEquals(1000, node2.get("duration").asLong());

        JsonNode node3 = node.get(2);

        assertEquals(2, node3.get("counter").asLong());
        assertEquals(endTime2, node3.get("time").asLong());
        assertEquals(duration2 + duration3, node3.get("duration").asLong());
    }

    /**
     * @throws Exception If failed.
     */
    private JsonNode readResultStatistics(IgnitePerformanceStatisticsHandler hnd)
        throws Exception {
        File perfStatDir = new File(U.defaultWorkDirectory(), PERF_STAT_DIR);

        assertTrue(perfStatDir.exists());

        new FilePerformanceStatisticsReader(hnd).read(Collections.singletonList(perfStatDir));

        ObjectNode dataJson = MAPPER.createObjectNode();

        hnd.results().forEach(dataJson::set);

        return dataJson;
    }
}
