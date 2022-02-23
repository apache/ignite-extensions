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
import java.util.Collection;
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
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteExperimental;
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
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_MAX_BATCH_SIZE;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_PARTS;
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
@IgniteExperimental
public class IgniteToKafkaCdcStreamer implements CdcConsumer {
    /** Default value for the flag that indicates whether entries only from primary nodes should be handled. */
    public static final boolean DFLT_IS_ONLY_PRIMARY = false;

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
    private boolean onlyPrimary = DFLT_IS_ONLY_PRIMARY;

    /** Topic name. */
    private String topic;

    /** Kafka topic partitions count. */
    private int kafkaParts = DFLT_PARTS;

    /** Kafka properties. */
    private Properties kafkaProps;

    /** Cache IDs. */
    private Set<Integer> cachesIds;

    /** Cache names. */
    private Collection<String> cacheNames;

    /** Max batch size. */
    private int maxBatchSize = DFLT_MAX_BATCH_SIZE;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private int kafkaReqTimeout = DFLT_KAFKA_REQ_TIMEOUT;

    /** Timestamp of last sent message. */
    private AtomicLongMetric lastMsgTs;

    /** Count of bytes sent to the Kafka. */
    private AtomicLongMetric bytesSnt;

    /** Count of sent messages.  */
    private AtomicLongMetric msgsSnt;

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

            if (!cachesIds.contains(evt.cacheId())) {
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
                    fut.get(kafkaReqTimeout, TimeUnit.MILLISECONDS);

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
        A.notNull(kafkaProps, "Kafka properties");
        A.notNull(topic, "Kafka topic");
        A.notEmpty(cacheNames, "caches");
        A.ensure(kafkaParts > 0, "The number of Kafka partitions must be greater than zero.");
        A.ensure(kafkaReqTimeout >= 0, "The Kafka request timeout cannot be negative.");

        kafkaProps.setProperty(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProps.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        cachesIds = cacheNames.stream()
            .map(CU::cacheId)
            .collect(Collectors.toSet());

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

    /** @return Whether entries only from primary nodes should be handled. */
    public boolean isOnlyPrimary() {
        return onlyPrimary;
    }

    /**
     * Sets whether entries only from primary nodes should be handled.
     *
     * @param onlyPrimary Kafka whether entries only from primary nodes should be handled.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setOnlyPrimary(boolean onlyPrimary) {
        this.onlyPrimary = onlyPrimary;

        return this;
    }

    /** @return Topic that is used to send data to Kafka. */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets topic that is used to send data to Kafka.
     *
     * @param topic Kafka topic.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setTopic(String topic) {
        this.topic = topic;

        return this;
    }

    /** @return Number of Kafka partitions. */
    public int getKafkaPartitions() {
        return kafkaParts;
    }

    /**
     * Sets number of Kafka partitions.
     *
     * @param kafkaParts Number of Kafka partitions.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setKafkaPartitions(int kafkaParts) {
        this.kafkaParts = kafkaParts;

        return this;
    }

    /** @return Cache names that participate in CDC. */
    public Collection<String> getCaches() {
        return cacheNames;
    }

    /**
     * Sets cache names that participate in CDC.
     *
     * @param caches Cache names.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setCaches(Collection<String> caches) {
        this.cacheNames = caches;

        return this;
    }

    /** @return Maximum batch size. */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /**
     * Sets maximum batch size.
     *
     * @param maxBatchSize Maximum batch size.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;

        return this;
    }

    /** @return Properties that are used to initiate connection to Kafka. */
    public Properties getKafkaProperties() {
        return kafkaProps;
    }

    /**
     * Sets properties that are used to initiate connection to Kafka.
     *
     * @param kafkaProps Properties that are used to initiate connection to Kafka.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setKafkaProperties(Properties kafkaProps) {
        this.kafkaProps = kafkaProps;

        return this;
    }

    /** @return The maximum time to complete Kafka related requests, in milliseconds. */
    public int getKafkaRequestTimeout() {
        return kafkaReqTimeout;
    }

    /**
     * Sets the maximum time to complete Kafka related requests, in milliseconds.
     * 
     * @param kafkaReqTimeout Timeout value.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setKafkaRequestTimeout(int kafkaReqTimeout) {
        this.kafkaReqTimeout = kafkaReqTimeout;

        return this;
    }
}
