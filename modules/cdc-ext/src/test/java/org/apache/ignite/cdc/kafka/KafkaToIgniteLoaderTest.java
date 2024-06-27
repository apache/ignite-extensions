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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteLoader.loadKafkaToIgniteStreamer;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Tests load {@link KafkaToIgniteCdcStreamer} from Srping xml file. */
public class KafkaToIgniteLoaderTest extends GridCommonAbstractTest {
    /** Constant to reference from xml config. */
    public static final int TEST_KAFKA_CONSUMER_POLL_TIMEOUT = 2000;

    /** Constant to reference from xml config. */
    public static final int TEST_KAFKA_REQUEST_TIMEOUT = 6000;

    /** */
    @Test
    public void testLoadConfig() throws Exception {
        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/kafka-to-ignite-double-ignite-cfg.xml"),
            IgniteCheckedException.class,
            "Exact 1 IgniteConfiguration should be defined. Found 2"
        );

        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/kafka-to-ignite-without-kafka-properties.xml"),
            IgniteCheckedException.class,
            "Spring bean with provided name doesn't exist"
        );

        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/same-consumers-group/kafka-to-ignite.xml"),
            IllegalArgumentException.class,
            "The group of event and metadata consumers must be different."
        );

        KafkaToIgniteCdcStreamer streamer = loadKafkaToIgniteStreamer("loader/kafka-to-ignite-correct.xml");

        assertNotNull(streamer);
    }

    /** */
    @Test
    public void testLoadIgniteClientConfig() throws Exception {
        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-double-client-cfg.xml"),
            IgniteCheckedException.class,
            "Exact 1 ClientConfiguration should be defined. Found 2"
        );

        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-without-kafka-properties.xml"),
            IgniteCheckedException.class,
            "Spring bean with provided name doesn't exist"
        );

        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-with-ignite-cfg.xml"),
            IgniteCheckedException.class,
            "Either IgniteConfiguration or ClientConfiguration should be defined."
        );

        KafkaToIgniteClientCdcStreamer streamer = loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-correct.xml");

        assertNotNull(streamer);
    }

    /** Tests setting timeout properties of kafka to ignite loaders. */
    @Test
    public void testLoadTimeoutProperties() throws Exception {
        Stream.of(
            new String[] {
                "loader/thin/kafka-to-ignite-client-invalid-poll-timeout.xml",
                "Ouch! Argument is invalid: The Kafka consumer poll timeout cannot be negative."},
            new String[] {
                "loader/thin/kafka-to-ignite-client-invalid-request-timeout.xml",
                "Ouch! Argument is invalid: The Kafka request timeout cannot be negative."
            },
            new String[] {
                "loader/kafka-to-ignite-invalid-poll-timeout.xml",
                "Ouch! Argument is invalid: The Kafka consumer poll timeout cannot be negative."},
            new String[] {
                "loader/kafka-to-ignite-invalid-request-timeout.xml",
                "Ouch! Argument is invalid: The Kafka request timeout cannot be negative."
            }
        ).forEach(args -> assertThrows(null, () -> loadKafkaToIgniteStreamer(args[0]), IllegalArgumentException.class, args[1]));

        Stream.<AbstractKafkaToIgniteCdcStreamer>of(
            loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-correct.xml"),
            loadKafkaToIgniteStreamer("loader/kafka-to-ignite-correct.xml")
        ).forEach(streamer -> {
            assertNotNull(streamer);
            assertEquals(TEST_KAFKA_CONSUMER_POLL_TIMEOUT, streamer.streamerCfg.getKafkaConsumerPollTimeout());
            assertEquals(TEST_KAFKA_REQUEST_TIMEOUT, streamer.streamerCfg.getKafkaRequestTimeout());
        });
    }

    /** Tests setting kafka properties of kafka to ignite loaders. */
    @Test
    public void testKafkaProperties() {
        Stream.of(
            new String[] {
                "loader/thin/kafka-to-ignite-client-without-topic.xml",
                "Ouch! Argument cannot be null: Kafka topic"},
            new String[] {
                "loader/thin/kafka-to-ignite-client-without-metadata-topic.xml",
                "Ouch! Argument cannot be null: Kafka metadata topic"},
            new String[] {
                "loader/kafka-to-ignite-without-topic.xml",
                "Ouch! Argument cannot be null: Kafka topic"},
            new String[] {
                "loader/kafka-to-ignite-without-metadata-topic.xml",
                "Ouch! Argument cannot be null: Kafka metadata topic"}
        ).forEach(args -> assertThrows(null, () -> loadKafkaToIgniteStreamer(args[0]), NullPointerException.class, args[1]));

        Stream.of(
            new String[] {
                "loader/thin/kafka-to-ignite-client-with-negative-partition-from.xml",
                "Ouch! Argument is invalid: The Kafka partitions lower bound must be explicitly set to a value greater" +
                    " than or equals to zero."},
            new String[] {
                "loader/thin/kafka-to-ignite-client-with-negative-partition-to.xml",
                "Ouch! Argument is invalid: The Kafka partitions upper bound must be explicitly set to a value greater" +
                    " than zero."},
            new String[] {
                "loader/thin/kafka-to-ignite-client-with-incorrect-partition-distribution.xml",
                "Ouch! Argument is invalid: The Kafka partitions upper bound must be greater than lower bound."},
            new String[] {
                "loader/thin/kafka-to-ignite-client-with-negative-thread-count.xml",
                "Ouch! Argument is invalid: Threads count value must me greater than zero."},
            new String[] {
                "loader/thin/kafka-to-ignite-client-with-incorrect-thread-count.xml",
                "Ouch! Argument is invalid: Threads count must be less or equals to the total Kafka partitions count."},
            new String[] {
                "loader/kafka-to-ignite-with-negative-partition-from.xml",
                "Ouch! Argument is invalid: The Kafka partitions lower bound must be explicitly set to a value greater" +
                    " than or equals to zero."},
            new String[] {
                "loader/kafka-to-ignite-with-negative-partition-to.xml",
                "Ouch! Argument is invalid: The Kafka partitions upper bound must be explicitly set to a value greater" +
                    " than zero."},
            new String[] {
                "loader/kafka-to-ignite-with-incorrect-partition-distribution.xml",
                "Ouch! Argument is invalid: The Kafka partitions upper bound must be greater than lower bound."},
            new String[] {
                "loader/kafka-to-ignite-with-negative-thread-count.xml",
                "Ouch! Argument is invalid: Threads count value must me greater than zero."},
            new String[] {
                "loader/kafka-to-ignite-with-incorrect-thread-count.xml",
                "Ouch! Argument is invalid: Threads count must be less or equals to the total Kafka partitions count."}
        ).forEach(args -> assertThrows(null, () -> loadKafkaToIgniteStreamer(args[0]), IllegalArgumentException.class, args[1]));
    }

    /** */
    @Test
    public void testInitSpringContextOnce() throws Exception {
        assertEquals(0, InitiationTestBean.initCnt.get());

        loadKafkaToIgniteStreamer("loader/kafka-to-ignite-initiation-context-test.xml");

        assertEquals(1, InitiationTestBean.initCnt.get());
    }

    /** */
    private static class InitiationTestBean {
        /** */
        static AtomicInteger initCnt = new AtomicInteger();

        /** */
        InitiationTestBean() {
            initCnt.incrementAndGet();
        }
    }
}
