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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.LoggerResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.EVTS_CNT;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.EVTS_CNT_DESC;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.LAST_EVT_TIME;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.LAST_EVT_TIME_DESC;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Change Data Consumer that streams all data changes to Kafka topic.
 * {@link CdcEvent} spread across Kafka topic partitions with {@code {ignite_partition} % {kafka_topic_count}} formula.
 * Consumer will just fail in case of any error during write. Fail of consumer will lead to the fail of {@code ignite-cdc} application.
 * It expected that {@code ignite-cdc} will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka unavailability or network issues.
 *
 * If you have plans to apply written messages to the other Ignite cluster in active-active manner,
 * e.g. concurrent updates of the same entry in other cluster is possible,
 * please, be aware of {@link CacheVersionConflictResolverImpl} conflict resolved.
 * Configuration of {@link CacheVersionConflictResolverImpl} can be found in {@link KafkaToIgniteCdcStreamer} documentation.
 *
 * @see CdcMain
 * @see KafkaToIgniteCdcStreamer
 * @see CacheVersionConflictResolverImpl
 */
public class IgniteToKafkaCdcStreamer implements CdcConsumer {
    /** Default kafka request timeout in seconds. */
    public static final int DFLT_REQ_TIMEOUT = 5;

    /** Bytes sent metric name. */
    public static final String BYTES_SENT = "BytesSent";

    /** Bytes sent metric description. */
    public static final String BYTES_SENT_DESCRIPTION = "Count of bytes sent.";

    /** Log. */
    @LoggerResource
    private IgniteLogger log;

    /** Kafka producer to stream events. */
    private KafkaProducer<Integer, byte[]> producer;

    /** Handle only primary entry flag. */
    private final boolean onlyPrimary;

    /** Topic name. */
    private final String topic;

    /** Kafka topic partitions count. */
    private final int kafkaParts;

    /** Kafka properties. */
    private final Properties kafkaProps;

    /** Cache IDs. */
    private final Set<Integer> cachesIds;

    /** Max batch size. */
    private final int maxBatchSize;

    /** Timestamp of last sent message. */
    private AtomicLongMetric lastMsgTs;

    /** Count of bytes sent to the Kafka. */
    private AtomicLongMetric bytesSnt;

    /** Count of sent messages.  */
    private AtomicLongMetric msgsSnt;

    /**
     * @param topic Topic name.
     * @param kafkaParts Kafka partitions count.
     * @param caches Cache names.
     * @param maxBatchSize Maximum size of records concurrently sent to Kafka.
     * @param onlyPrimary If {@code true} then stream only events from primaries.
     * @param kafkaProps Kafka properties.
     */
    public IgniteToKafkaCdcStreamer(
        String topic,
        int kafkaParts,
        Set<String> caches,
        int maxBatchSize,
        boolean onlyPrimary,
        Properties kafkaProps
    ) {
        assert caches != null && !caches.isEmpty();

        this.topic = topic;
        this.kafkaParts = kafkaParts;
        this.onlyPrimary = onlyPrimary;
        this.kafkaProps = kafkaProps;
        this.maxBatchSize = maxBatchSize;

        cachesIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());

        kafkaProps.setProperty(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProps.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> evts) {
        List<Future<RecordMetadata>> futs = new ArrayList<>();

        while (evts.hasNext() && futs.size() < maxBatchSize) {
            CdcEvent evt = evts.next();

            if (log.isDebugEnabled())
                log.debug("Event received [evt=" + evt + ']');

            if (onlyPrimary && !evt.primary()) {
                if (log.isDebugEnabled())
                    log.debug("Event skipped because of primary flag [evt=" + evt + ']');

                continue;
            }

            if (evt.version().otherClusterVersion() != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Event skipped because of version [evt=" + evt +
                        ", otherClusterVersion=" + evt.version().otherClusterVersion() + ']');
                }

                continue;
            }

            if (!cachesIds.isEmpty() && !cachesIds.contains(evt.cacheId())) {
                if (log.isDebugEnabled())
                    log.debug("Event skipped because of cacheId [evt=" + evt + ']');

                continue;
            }

            byte[] bytes = IgniteUtils.toBytes(evt);

            bytesSnt.add(bytes.length);

            futs.add(producer.send(new ProducerRecord<>(
                topic,
                evt.partition() % kafkaParts,
                evt.cacheId(),
                bytes
            )));

            if (log.isDebugEnabled())
                log.debug("Event sent asynchronously [evt=" + evt + ']');
        }

        if (!futs.isEmpty()) {
            try {
                for (Future<RecordMetadata> fut : futs)
                    fut.get(DFLT_REQ_TIMEOUT, TimeUnit.SECONDS);

                msgsSnt.add(futs.size());

                lastMsgTs.value(System.currentTimeMillis());
            }
            catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }

            if (log.isInfoEnabled())
                log.info("Events processed [sentMessagesCount=" + msgsSnt.value() + ']');
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        try {
            producer = new KafkaProducer<>(kafkaProps);

            if (log.isInfoEnabled())
                log.info("CDC Ignite To Kafka started [topic=" + topic + ", onlyPrimary=" + onlyPrimary + ", cacheIds=" + cachesIds + ']');
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.msgsSnt = mreg.longMetric(EVTS_CNT, EVTS_CNT_DESC);
        this.lastMsgTs = mreg.longMetric(LAST_EVT_TIME, LAST_EVT_TIME_DESC);
        this.bytesSnt = mreg.longMetric(BYTES_SENT, BYTES_SENT_DESCRIPTION);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        producer.close();
    }
}
