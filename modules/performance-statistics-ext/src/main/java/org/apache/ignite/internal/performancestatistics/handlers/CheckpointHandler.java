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
import org.apache.ignite.internal.util.typedef.internal.U;

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

        /** */
        public CheckpointInfo(UUID nodeId, long beforeLockDuration, long lockWaitDuration, long listenersExecDuration,
            long markDuration, long lockHoldDuration, long pagesWriteDuration, long fsyncDuration,
            long walCpRecordFsyncDuration, long writeCheckpointEntryDuration, long splitAndSortCpPagesDuration,
            long totalDuration, long cpStartTime, int pagesSize, int dataPagesWritten, int cowPagesWritten) {
            this.nodeId = nodeId;
            this.beforeLockDuration = beforeLockDuration;
            this.lockWaitDuration = lockWaitDuration;
            this.listenersExecDuration = listenersExecDuration;
            this.markDuration = markDuration;
            this.lockHoldDuration = lockHoldDuration;
            this.pagesWriteDuration = pagesWriteDuration;
            this.fsyncDuration = fsyncDuration;
            this.walCpRecordFsyncDuration = walCpRecordFsyncDuration;
            this.writeCheckpointEntryDuration = writeCheckpointEntryDuration;
            this.splitAndSortCpPagesDuration = splitAndSortCpPagesDuration;
            this.totalDuration = totalDuration;
            this.cpStartTime = cpStartTime;
            this.pagesSize = pagesSize;
            this.dataPagesWritten = dataPagesWritten;
            this.cowPagesWritten = cowPagesWritten;
        }

        public UUID getNodeId() {
            return nodeId;
        }

        public long getBeforeLockDuration() {
            return beforeLockDuration;
        }

        public long getLockWaitDuration() {
            return lockWaitDuration;
        }

        public long getListenersExecDuration() {
            return listenersExecDuration;
        }

        public long getMarkDuration() {
            return markDuration;
        }

        public long getLockHoldDuration() {
            return lockHoldDuration;
        }

        public long getPagesWriteDuration() {
            return pagesWriteDuration;
        }

        public long getFsyncDuration() {
            return fsyncDuration;
        }

        public long getWalCpRecordFsyncDuration() {
            return walCpRecordFsyncDuration;
        }

        public long getWriteCheckpointEntryDuration() {
            return writeCheckpointEntryDuration;
        }

        public long getSplitAndSortCpPagesDuration() {
            return splitAndSortCpPagesDuration;
        }

        public long getTotalDuration() {
            return totalDuration;
        }

        public long getCpStartTime() {
            return cpStartTime;
        }

        public int getPagesSize() {
            return pagesSize;
        }

        public int getDataPagesWritten() {
            return dataPagesWritten;
        }

        public int getCowPagesWritten() {
            return cowPagesWritten;
        }
    }

    /**
     *
     */
    private static class ThrottlesInfo {
        /** */
        UUID nodeId;

        /** */
        long time;

        /** */
        long counter;

        /** */
        long duration;

        /** */
        public ThrottlesInfo(UUID nodeId, long time, long counter, long duration) {
            this.nodeId = nodeId;
            this.time = time;
            this.counter = counter;
            this.duration = duration;
        }

        public UUID getNodeId() {
            return nodeId;
        }

        public long getTime() {
            return time;
        }

        public long getCounter() {
            return counter;
        }

        public long getDuration() {
            return duration;
        }
    }
}
