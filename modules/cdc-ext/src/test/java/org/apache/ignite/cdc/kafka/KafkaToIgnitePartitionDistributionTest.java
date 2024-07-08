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
