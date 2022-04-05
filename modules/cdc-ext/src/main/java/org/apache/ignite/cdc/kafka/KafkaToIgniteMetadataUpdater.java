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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.CdcEventsApplier;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.MetaType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.META_TYPE_HEADER;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaToIgniteMetadataUpdater implements AutoCloseable, Runnable {
    /** Ignite instance. */
    private final IgniteEx ign;

    /** Log. */
    private final IgniteLogger log;

    /** Closed flag. Shared between all appliers. */
    private final AtomicBoolean stopped;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private final long kafkaReqTimeout;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private final long metaUpdInterval;

    /** */
    private final KafkaConsumer<Void, byte[]> cnsmr;

    /** */
    private final AtomicLong rcvdEvts = new AtomicLong();

    /**
     * @param ign Ignite instance.
     * @param log Logger.
     * @param initProps Kafka properties.
     * @param streamerCfg Streamer configuration.
     * @param stopped Stopped flag.
     */
    public KafkaToIgniteMetadataUpdater(
        IgniteEx ign,
        IgniteLogger log,
        Properties initProps,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg,
        AtomicBoolean stopped
    ) {
        this.ign = ign;
        this.kafkaReqTimeout = streamerCfg.getKafkaRequestTimeout();
        this.metaUpdInterval = streamerCfg.getMetaUpdateInterval();
        this.stopped = stopped;
        this.log = log.getLogger(KafkaToIgniteMetadataUpdater.class);

        Properties kafkaProps = new Properties();

        kafkaProps.putAll(initProps);
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProps.put(GROUP_ID_CONFIG, streamerCfg.getMetaConsumerGroup() != null
            ? streamerCfg.getMetaConsumerGroup()
            : ("ignite-metadata-update-" + ThreadLocalRandom.current().nextInt()));

        cnsmr = new KafkaConsumer<>(kafkaProps);

        cnsmr.subscribe(Collections.singletonList(streamerCfg.getMetadataTopic()));
    }

    /** {@inheritDoc} */
    @Override public void run() {
        U.setCurrentIgniteName(ign.name());

        while (!stopped.get()) {
            updateMetadata();

            try {
                Thread.sleep(metaUpdInterval);
            }
            catch (InterruptedException e) {
                // Ignore.
            }
        }
    }

    /** Polls all available records from metadata topic and applies it to Ignite. */
    public synchronized void updateMetadata() {
        while (true) {
            ConsumerRecords<Void, byte[]> recs = cnsmr.poll(Duration.ofMillis(kafkaReqTimeout));

            if (recs.count() == 0)
                return;

            if (log.isInfoEnabled())
                log.info("Polled from meta topic [rcvdEvts=" + rcvdEvts.addAndGet(recs.count()) + ']');

            for (ConsumerRecord<Void, byte[]> rec : recs) {
                byte[] bytes = rec.headers().lastHeader(META_TYPE_HEADER).value();

                assert bytes != null;

                MetaType type = MetaType.valueOf(new String(bytes, UTF_8));

                switch (type) {
                    case BINARY:
                        BinaryMetadata meta = IgniteUtils.fromBytes(rec.value());

                        CdcEventsApplier.registerBinaryMeta(ign, meta);

                        if (log.isInfoEnabled())
                            log.info("BinaryMeta[meta=" + meta + ']');

                        break;
                    case MAPPINGS:
                        TypeMapping m = IgniteUtils.fromBytes(rec.value());

                        CdcEventsApplier.registerMapping(ign, m);

                        if (log.isInfoEnabled())
                            log.info("Mapping[mapping=" + m + ']');

                        break;
                    default:
                        throw new IllegalArgumentException("Unknown meta type[type=" + type + ']');
                }
            }

            cnsmr.commitSync(Duration.ofMillis(kafkaReqTimeout));
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.warning("Close applier!");

        cnsmr.wakeup();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(KafkaToIgniteMetadataUpdater.class, this);
    }
}
