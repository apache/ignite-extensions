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

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.CdcUtils;
import org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.MetaType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.META_TYPE_HEADER;

public class KafkaToIgniteMetadataApplier implements AutoCloseable, Runnable {
    /** Ignite instance. */
    private final IgniteEx ign;

    /** Log. */
    private final IgniteLogger log;

    /** Closed flag. Shared between all appliers. */
    private final AtomicBoolean stopped;

    /** Kafka properties. */
    private final Properties kafkaProps;

    /** Topic to read. */
    private final String topic;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private final long kafkaReqTimeout;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private final long metaUpdInterval;

    private KafkaConsumer<Void, byte[]> cnsmr;

    /** */
    private final AtomicLong rcvdEvts = new AtomicLong();

    /**
     * @param ign Ignite instance.
     * @param log Logger.
     * @param kafkaProps Kafka properties.
     * @param topic Topic name.
     * @param kafkaReqTimeout The maximum time to complete Kafka related requests, in milliseconds.
     * @param metaUpdTimeout Amount of time between two polling.
     * @param stopped Stopped flag.
     */
    public KafkaToIgniteMetadataApplier(
        IgniteEx ign,
        IgniteLogger log,
        Properties kafkaProps,
        String topic,
        long kafkaReqTimeout,
        long metaUpdTimeout,
        AtomicBoolean stopped
    ) {
        this.ign = ign;
        this.kafkaProps = kafkaProps;
        this.topic = topic;
        this.kafkaReqTimeout = kafkaReqTimeout;
        this.metaUpdInterval = metaUpdTimeout;
        this.stopped = stopped;
        this.log = log.getLogger(KafkaToIgniteCdcStreamerApplier.class);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        U.setCurrentIgniteName(ign.name());

        //TODO: add group.id and serde check for kafkaprops.
        cnsmr = new KafkaConsumer<>(kafkaProps);

        cnsmr.subscribe(Collections.singletonList(topic));

        while (!stopped.get()) {
            ConsumerRecords<Void, byte[]> recs = cnsmr.poll(Duration.ofMillis(kafkaReqTimeout));

            if (log.isDebugEnabled()) {
                log.debug("Polled from meta consumer [assignments=" + cnsmr.assignment() +
                    ",rcvdEvts=" + rcvdEvts.addAndGet(recs.count()) + ']');
            }

            for (ConsumerRecord<Void, byte[]> rec : recs) {
                byte[] bytes = rec.headers().lastHeader(META_TYPE_HEADER).value();

                assert bytes != null;

                MetaType type = MetaType.valueOf(new String(bytes, UTF_8));

                switch (type) {
                    case BINARY:
                        CdcUtils.registerBinaryMeta(ign, IgniteUtils.fromBytes(rec.value()));

                        break;
                    case MAPPINGS:
                        CdcUtils.registerMapping(ign, IgniteUtils.fromBytes(rec.value()));

                        break;
                    default:
                        throw new IllegalArgumentException("Unknown meta type[type=" + type + ']');
                }
            }

            cnsmr.commitSync(Duration.ofMillis(kafkaReqTimeout));

            try {
                Thread.sleep(metaUpdInterval);
            }
            catch (InterruptedException e) {
                // Ignore.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.warning("Close applier!");

        cnsmr.wakeup();
    }
}
