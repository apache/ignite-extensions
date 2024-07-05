package org.apache.ignite.cdc.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.kafka.AbstractKafkaToIgniteCdcStreamer.kafkaPartitions;

/** Tests the kafka topic partitions distribution over specified number of threads. */
@RunWith(Parameterized.class)
public class KafkaToIgnitePartitionDistributionTest extends GridCommonAbstractTest {
    /** Streamer config. */
    private static KafkaToIgniteCdcStreamerConfiguration streamerCfg;

    /** Thread count. */
    @Parameterized.Parameter()
    public Integer threadCnt;

    /** Test result. */
    @Parameterized.Parameter(1)
    public List<IgniteBiTuple<Integer, Integer>> result;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "threadCnt={0}, result={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        params.add(new Object[] {3, Arrays.asList(t(0, 3), t(3, 6), t(6, 8))});
        params.add(new Object[] {4, Arrays.asList(t(0, 2), t(2, 4), t(4, 6), t(6, 8))});
        params.add(new Object[] {5, Arrays.asList(t(0, 2), t(2, 4), t(4, 6), t(6, 7), t(7, 8))});

        return params;
    }

    /**
     * Shortcut method for test parameters' creation.
     * @param l {@code int} starting partition.
     * @param r {@code int} ending partition.
     */
    private static IgniteBiTuple<Integer, Integer> t(int l, int r) {
        return new IgniteBiTuple<>(l, r);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() {
        streamerCfg = new KafkaToIgniteCdcStreamerConfiguration();

        streamerCfg.setKafkaPartsFrom(0);
        streamerCfg.setKafkaPartsTo(8);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        streamerCfg.setThreadCount(threadCnt);
    }

    /** Checks the topic partitions distribution over specified number of threads. */
    @Test
    public void testActivePassiveReplication() {
        assertEquals(result, kafkaPartitions(streamerCfg));
    }
}
