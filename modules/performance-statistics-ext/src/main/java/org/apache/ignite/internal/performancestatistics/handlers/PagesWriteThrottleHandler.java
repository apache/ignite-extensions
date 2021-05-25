package org.apache.ignite.internal.performancestatistics.handlers;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Builds JSON with pagesWriteThrottle information.
 *
 * Example:
 * <pre>
 * {
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
public class PagesWriteThrottleHandler implements IgnitePerformanceStatisticsHandler{
    /** Pages write throttle: nodeId -> time -> throttlesInfo. */
    private final Map<UUID, Map<Long, PagesWriteThrottleInfo>> pagesWriteThrottle = new HashMap<>();

    /** */
    public static final String PAGES_WRITE_THROTTLE = "pagesWriteThrottle";


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

        return U.map(PAGES_WRITE_THROTTLE, MAPPER.valueToTree(pagesWriteThrottle));
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
