package org.apache.ignite.cdc.kafka;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cdc.kafka.AbstractKafkaToIgniteCdcStreamer.kafkaPartitions;

/** Tests Kafka partition ranges assignment to the multiple applier threads. */
public class KafkaToIgnitePartitionDistributionTest extends GridCommonAbstractTest {
    /** */
    private final KafkaToIgniteCdcStreamerConfiguration streamerCfg = new KafkaToIgniteCdcStreamerConfiguration();

    /** */
    @Test
    public void testKafkaPartitions() {
        doTest(3, 0, 8, Arrays.asList(F.t(0, 3), F.t(3, 6), F.t(6, 8)));
        doTest(4, 0, 8, Arrays.asList(F.t(0, 2), F.t(2, 4), F.t(4, 6), F.t(6, 8)));
        doTest(5, 0, 8, Arrays.asList(F.t(0, 2), F.t(2, 4), F.t(4, 6), F.t(6, 7), F.t(7, 8)));

        doTest(3, 3, 11, Arrays.asList(F.t(3, 6), F.t(6, 9), F.t(9, 11)));
        doTest(4, 3, 11, Arrays.asList(F.t(3, 5), F.t(5, 7), F.t(7, 9), F.t(9, 11)));
        doTest(5, 3, 11, Arrays.asList(F.t(3, 5), F.t(5, 7), F.t(7, 9), F.t(9, 10), F.t(10, 11)));

        doTest(3, 1, 4, Arrays.asList(F.t(1, 2), F.t(2, 3), F.t(3, 4)));
    }

    /**
     * @param threadCnt Applier threads count.
     * @param expParts List of expected partition ranges.
     */
    private void doTest(int threadCnt, int partFrom, int partTo, List<IgniteBiTuple<Integer, Integer>> expParts) {
        streamerCfg.setThreadCount(threadCnt);
        streamerCfg.setKafkaPartsFrom(partFrom);
        streamerCfg.setKafkaPartsTo(partTo);

        assertEquals(expParts, kafkaPartitions(streamerCfg));
    }
}
