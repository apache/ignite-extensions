package org.apache.ignite.cdc.kafka;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cdc.kafka.AbstractKafkaToIgniteCdcStreamer.kafkaPartitions;

/** Tests the kafka topic partitions distribution over specified number of threads. */
public class KafkaToIgnitePartitionDistributionTest extends GridCommonAbstractTest {
    /** Streamer config. */
    private final KafkaToIgniteCdcStreamerConfiguration streamerCfg = new KafkaToIgniteCdcStreamerConfiguration();

    /** Checks the topic partitions distribution over specified number of threads. */
    @Test
    public void testKafkaPartitions() {
        testKafkaPartitionsCase(3, 0, 8, Arrays.asList(F.t(0, 3), F.t(3, 6), F.t(6, 8)));
        testKafkaPartitionsCase(4, 0, 8, Arrays.asList(F.t(0, 2), F.t(2, 4), F.t(4, 6), F.t(6, 8)));
        testKafkaPartitionsCase(5, 0, 8, Arrays.asList(F.t(0, 2), F.t(2, 4), F.t(4, 6), F.t(6, 7), F.t(7, 8)));

        testKafkaPartitionsCase(3, 3, 11, Arrays.asList(F.t(3, 6), F.t(6, 9), F.t(9, 11)));
        testKafkaPartitionsCase(4, 3, 11, Arrays.asList(F.t(3, 5), F.t(5, 7), F.t(7, 9), F.t(9, 11)));
        testKafkaPartitionsCase(5, 3, 11, Arrays.asList(F.t(3, 5), F.t(5, 7), F.t(7, 9), F.t(9, 10), F.t(10, 11)));

        testKafkaPartitionsCase(3, 1, 4, Arrays.asList(F.t(1, 2), F.t(2, 3), F.t(3, 4)));
    }

    /**
     * Checks the topic partitions distribution over specified number of threads for specified parameters.
     * @param threadCnt Thread count.
     * @param result List of pairs defining partition ranges for each applier thread.
     */
    private void testKafkaPartitionsCase(int threadCnt, int partFrom, int partTo, List<IgniteBiTuple<Integer, Integer>> result) {
        streamerCfg.setThreadCount(threadCnt);
        streamerCfg.setKafkaPartsFrom(partFrom);
        streamerCfg.setKafkaPartsTo(partTo);

        assertEquals(result, kafkaPartitions(streamerCfg));
    }
}
