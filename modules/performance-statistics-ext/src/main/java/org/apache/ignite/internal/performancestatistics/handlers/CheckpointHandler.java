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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
 *      "pagesWriteThrottle": [
 *          {
 *              "nodeId" : $nodeId,
 *              "time" : $time,
 *              "counter" : $counter,
 *              "duration" : $duration
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
    private final LinkedList<CheckpointInfo> checkpoints = new LinkedList<>();

    /** Pages write throttle: nodeId -> time -> throttlesInfo. */
    private final Map<UUID, Map<Long, PagesWriteThrottleInfo>> pagesWriteThrottle = new HashMap<>();

    /** */
    public static final String CHECKPOINTS_INFO = "checkpointsInfo";

    /** */
    public static final String CHECKPOINTS = "checkpoints";

    /** */
    public static final String PAGES_WRITE_THROTTLE = "pagesWriteThrottle";

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
        long durationSec = TimeUnit.MILLISECONDS.toSeconds(duration);

        if (durationSec > 0)
            LongStream.range(0, durationSec + 1)
                .forEach(d -> addThrottlesInfo(nodeId, TimeUnit.SECONDS.toMillis(time - d), 1000));
        else
            addThrottlesInfo(nodeId, TimeUnit.SECONDS.toMillis(time), duration);
    }

    /**
     * @param nodeId Node id.
     * @param time Time in milliseconds.
     * @param duration Duration in milliseconds.
     */
    private void addThrottlesInfo(UUID nodeId, long time, long duration) {
        PagesWriteThrottleInfo info = pagesWriteThrottle.computeIfAbsent(nodeId, uuid -> new HashMap<>())
            .computeIfAbsent(time, t -> new PagesWriteThrottleInfo(nodeId, time, 0, 0));

        info.incrementCounter();
        info.addDuration(duration);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        List<PagesWriteThrottleInfo> pagesWriteThrottle = this.pagesWriteThrottle.values().stream()
            .map(Map::values)
            .flatMap(Collection::stream)
            .sorted(Comparator.comparingLong(PagesWriteThrottleInfo::getTime))
            .collect(Collectors.toList());

        res.set(CHECKPOINTS, MAPPER.valueToTree(checkpoints));
        res.set(PAGES_WRITE_THROTTLE, MAPPER.valueToTree(pagesWriteThrottle));

        return U.map(CHECKPOINTS_INFO, res);
    }

    /** */
    private static class CheckpointInfo {
        /** */
        private final UUID nodeId;

        /** */
        private final long beforeLockDuration;

        /** */
        private final long lockWaitDuration;

        /** */
        private final long listenersExecDuration;

        /** */
        private final long markDuration;

        /** */
        private final long lockHoldDuration;

        /** */
        private final long pagesWriteDuration;

        /** */
        private final long fsyncDuration;

        /** */
        private final long walCpRecordFsyncDuration;

        /** */
        private final long writeCheckpointEntryDuration;

        /** */
        private final long splitAndSortCpPagesDuration;

        /** */
        private final long totalDuration;

        /** */
        private final long cpStartTime;

        /** */
        private final int pagesSize;

        /** */
        private final int dataPagesWritten;

        /** */
        private final int cowPagesWritten;

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

        /** */
        public UUID getNodeId() {
            return nodeId;
        }

        /** */
        public long getBeforeLockDuration() {
            return beforeLockDuration;
        }

        /** */
        public long getLockWaitDuration() {
            return lockWaitDuration;
        }

        /** */
        public long getListenersExecDuration() {
            return listenersExecDuration;
        }

        /** */
        public long getMarkDuration() {
            return markDuration;
        }

        /** */
        public long getLockHoldDuration() {
            return lockHoldDuration;
        }

        /** */
        public long getPagesWriteDuration() {
            return pagesWriteDuration;
        }

        /** */
        public long getFsyncDuration() {
            return fsyncDuration;
        }

        /** */
        public long getWalCpRecordFsyncDuration() {
            return walCpRecordFsyncDuration;
        }

        /** */
        public long getWriteCheckpointEntryDuration() {
            return writeCheckpointEntryDuration;
        }

        /** */
        public long getSplitAndSortCpPagesDuration() {
            return splitAndSortCpPagesDuration;
        }

        /** */
        public long getTotalDuration() {
            return totalDuration;
        }

        /** */
        public long getCpStartTime() {
            return cpStartTime;
        }

        /** */
        public int getPagesSize() {
            return pagesSize;
        }

        /** */
        public int getDataPagesWritten() {
            return dataPagesWritten;
        }

        /** */
        public int getCowPagesWritten() {
            return cowPagesWritten;
        }
    }

    /** */
    private static class PagesWriteThrottleInfo {
        /** */
        private final UUID nodeId;

        /** */
        private final long time;

        /** */
        private long cnt;

        /** */
        private long duration;

        /** */
        public PagesWriteThrottleInfo(UUID nodeId, long time, long cnt, long duration) {
            this.nodeId = nodeId;
            this.time = time;
            this.cnt = cnt;
            this.duration = duration;
        }

        /** */
        public UUID getNodeId() {
            return nodeId;
        }

        /** */
        public long getTime() {
            return time;
        }

        /** */
        public long getCounter() {
            return cnt;
        }

        /** */
        public void incrementCounter() {
            cnt++;
        }

        /** */
        public long getDuration() {
            return duration;
        }

        /** */
        public void addDuration(long duration) {
             this.duration += duration;
        }
    }
}
