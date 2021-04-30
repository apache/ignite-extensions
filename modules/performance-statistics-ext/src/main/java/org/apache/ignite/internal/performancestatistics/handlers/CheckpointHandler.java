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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import org.apache.ignite.internal.util.typedef.internal.U;

import lombok.Data;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Builds JSON with checkpoint and pagesWriteThrottle information.
 *
 * Example:
 * <pre>
 * {
 *      "checkpoints" : [
 *          {
 *             "nodeId" : $nodeId,
 *             "beforeLockDuration" : $beforeLockDuration,
 *             "lockWaitDuration" : $lockWaitDuration,
 *             "listenersExecDuration": $listenersExecDuration,
 *             "markDuration" : $markDuration,
 *             "lockHoldDuration" : $lockHoldDuration,
 *             "pagesWriteDuration" : $pagesWriteDuration,
 *             "fsyncDuration" : $fsyncDuration,
 *             "walCpRecordFsyncDuration" : $walCpRecordFsyncDuration,
 *             "writeCheckpointEntryDuration" : $writeCheckpointEntryDuration,
 *             "splitAndSortCpPagesDuration" : $splitAndSortCpPagesDuration,
 *             "totalDuration" : $totalDuration,
 *             "cpStartTime" : $cpStartTime,
 *             "pagesSize" : $pagesSize,
 *             "dataPagesWritten" : $dataPagesWritten,
 *             "cowPagesWritten" : $cowPagesWritten
 *          },
 *          ...
 *      ],
 *      "throttles": [
 *          {
 *              "nodeId" : $nodeId,
 *              "startTime" : $startTime,
 *              "endTime" : $endTime
 *          },
 *          ...
 *      ]
 * }
 * </pre>
 */
public class CheckpointHandler implements IgnitePerformanceStatisticsHandler {
    /** Result JSON. */
    private final ObjectNode res = MAPPER.createObjectNode();

    /** */
    private final List<CheckpointInfo> checkpoints = new ArrayList<>();

    /** */
    private final ArrayList<ThrottlesInfo> throttles = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void checkpoint(
        UUID nodeId,
        long beforeLockDuration,
        long lockWaitDuration,
        long listenersExecDuration,
        long markDuration,
        long lockHoldDuration,
        long pagesWriteDuration,
        long fsyncDuration,
        long walCpRecordFsyncDuration,
        long writeCpEntryDuration,
        long splitAndSortCpPagesDuration, long totalDuration,
        long cpStartTime,
        int pagesSize,
        int dataPagesWritten,
        int cowPagesWritten
    ) {
        checkpoints.add(new CheckpointInfo(
            nodeId,
            beforeLockDuration,
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
            cowPagesWritten)
        );
    }

    /** {@inheritDoc} */
    @Override public void pagesWriteThrottle(UUID nodeId, long endTime, long duration) {
        long time = TimeUnit.MILLISECONDS.toSeconds(endTime);

        if (!throttles.isEmpty()) {
            ThrottlesInfo last = throttles.get(throttles.size() - 1);

            if (last.nodeId == nodeId && TimeUnit.MILLISECONDS.toSeconds(last.time) == time) {

                last.counter++;
                last.duration+=duration;

                return;
            }
        }

        throttles.add(new ThrottlesInfo(nodeId, endTime, 1, duration));
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        res.set("checkpoints", MAPPER.valueToTree(checkpoints));
        res.set("throttles", MAPPER.valueToTree(throttles));

        return U.map("checkpointsInfo", res);
    }

    /**
     *
     */
    @Data
    @AllArgsConstructor
    private static class CheckpointInfo {
        /** */
        UUID nodeId;

        /** */
        long beforeLockDuration;

        /** */
        long lockWaitDuration;

        /** */
        long listenersExecDuration;

        /** */
        long markDuration;

        /** */
        long lockHoldDuration;

        /** */
        long pagesWriteDuration;

        /** */
        long fsyncDuration;

        /** */
        long walCpRecordFsyncDuration;

        /** */
        long writeCheckpointEntryDuration;

        /** */
        long splitAndSortCpPagesDuration;

        /** */
        long totalDuration;

        /** */
        long cpStartTime;

        /** */
        int pagesSize;

        /** */
        int dataPagesWritten;

        /** */
        int cowPagesWritten;
    }

    /**
     *
     */
    @Data
    @AllArgsConstructor
    private static class ThrottlesInfo {
        /** */
        UUID nodeId;

        /** */
        long time;

        /** */
        long counter;

        /** */
        long duration;
    }
}
