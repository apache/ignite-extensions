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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;

/**
 * Configuration of {@link KafkaToIgniteCdcStreamer} application.
 *
 * @see KafkaToIgniteCdcStreamer
 * @see KafkaToIgniteLoader
 */
public class KafkaToIgniteCdcStreamerConfiguration {
    /** Default maximum time to complete Kafka related requests, in milliseconds. */
    public static final long DFLT_KAFKA_REQ_TIMEOUT = 3_000L;

    /** Default kafka consumer poll timeout. */
    public static final long DFLT_KAFKA_CONSUMER_POLL_TIMEOUT = 3_000L;

    /** Default {@link #threadCnt} value. */
    public static final int DFLT_THREAD_CNT = 16;

    /** Default {@link #maxBatchSize} value. */
    public static final int DFLT_MAX_BATCH_SIZE = 1024;

    /** Default metrics registry name for Kafka to Ignite CDC. */
    public static final String DFLT_METRICS_REG_NAME = "cdc-kafka-to-ignite";

    /** {@link KafkaToIgniteCdcStreamerApplier} thread count. */
    private int threadCnt = DFLT_THREAD_CNT;

    /** Events topic name. */
    private String evtTopic;

    /** Metadata topic name. */
    private String metadataTopic;

    /** Kafka partitions lower bound (inclusive). */
    private int kafkaPartsFrom = -1;

    /** Kafka partitions higher bound (exclusive). */
    private int kafkaPartsTo;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private long kafkaReqTimeout = DFLT_KAFKA_REQ_TIMEOUT;

    /** Timeout of kafka consumer poll */
    private long kafkaConsumerPollTimeout = DFLT_KAFKA_CONSUMER_POLL_TIMEOUT;

    /** Metadata consumer group. */
    private String metadataCnsmrGrp;

    /**
     * Maximum batch size to apply to Ignite.
     *
     * @see IgniteInternalCache#putAllConflict(Map)
     * @see IgniteInternalCache#removeAllConflict(Map)
     */
    private int maxBatchSize = DFLT_MAX_BATCH_SIZE;

    /**
     * Cache names to process.
     */
    private Collection<String> caches;

    /** Metric exporter SPI. */
    private MetricExporterSpi[] metricExporterSpi;

    /** Metrics registry name for Kafka to Ignite CDC. */
    private String metricRegName;

    /**
     * @return Thread count.
     */
    public int getThreadCount() {
        return threadCnt;
    }

    /**
     * @param threadCnt Thread count.
     */
    public void setThreadCount(int threadCnt) {
        this.threadCnt = threadCnt;
    }

    /**
     * @return Topic.
     */
    public String getTopic() {
        return evtTopic;
    }

    /**
     * @param evtTopic Topic.
     */
    public void setTopic(String evtTopic) {
        this.evtTopic = evtTopic;
    }

    /**
     * @return Kafka partitions lower bound (inclusive).
     */
    public int getKafkaPartsFrom() {
        return kafkaPartsFrom;
    }

    /**
     * @param kafkaPartsFrom Kafka partitions lower bound (inclusive).
     */
    public void setKafkaPartsFrom(int kafkaPartsFrom) {
        this.kafkaPartsFrom = kafkaPartsFrom;
    }

    /**
     * @return Kafka partitions higher bound (exclusive).
     */
    public int getKafkaPartsTo() {
        return kafkaPartsTo;
    }

    /**
     * @param kafkaPartsTo Kafka partitions higher bound (exclusive).
     */
    public void setKafkaPartsTo(int kafkaPartsTo) {
        this.kafkaPartsTo = kafkaPartsTo;
    }

    /**
     * @return Maximum batch size to apply to Ignite.
     */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /**
     * @param maxBatchSize Maximum batch size to apply to Ignite.
     */
    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * @return Cache names to process.
     */
    public Collection<String> getCaches() {
        return caches;
    }

    /**
     * @param caches Cache names to process.
     */
    public void setCaches(Collection<String> caches) {
        this.caches = caches;
    }

    /** @return The maximum time to complete Kafka related requests, in milliseconds. */
    public long getKafkaRequestTimeout() {
        return kafkaReqTimeout;
    }

    /**
     * Sets the maximum time to complete Kafka related requests, in milliseconds.
     *
     * @param kafkaReqTimeout Timeout value.
     */
    public void setKafkaRequestTimeout(long kafkaReqTimeout) {
        this.kafkaReqTimeout = kafkaReqTimeout;
    }

    /**
     * @return The kafka consumer poll timeout in milliseconds.
     */
    public long getKafkaConsumerPollTimeout() {
        return kafkaConsumerPollTimeout;
    }

    /**
     * Sets the kafka consumer poll timeout in milliseconds.
     *
     * @param timeout Timeout value.
     */
    public void setKafkaConsumerPollTimeout(long timeout) {
        this.kafkaConsumerPollTimeout = timeout;
    }

    /**
     * @return Metadata topic name.
     */
    public String getMetadataTopic() {
        return metadataTopic;
    }

    /**
     * Sets metadata topic name.
     *
     * @param metadataTopic Metadata topic name.
     */
    public void setMetadataTopic(String metadataTopic) {
        this.metadataTopic = metadataTopic;
    }

    /**
     * @return Consumer group to read metadata topic.
     */
    public String getMetadataConsumerGroup() {
        return metadataCnsmrGrp;
    }

    /**
     * Sets consumer group to read metadata topic.
     *
     * @param metaCnsmrGrp Consumer group to read metadata topic.
     */
    public void setMetadataConsumerGroup(String metaCnsmrGrp) {
        this.metadataCnsmrGrp = metaCnsmrGrp;
    }

    /**
     * Sets fully configured instances of {@link MetricExporterSpi}. {@link JmxMetricExporterSpi} is used by default.
     *
     * @param metricExporterSpi Fully configured instances of {@link MetricExporterSpi}.
     * @see CdcConfiguration#getMetricExporterSpi()
     * @see JmxMetricExporterSpi
     */
    public void setMetricExporterSpi(MetricExporterSpi... metricExporterSpi) {
        this.metricExporterSpi = metricExporterSpi;
    }

    /**
     * Gets fully configured metric SPI implementations. {@link JmxMetricExporterSpi} is used by default.
     *
     * @return Metric exporter SPI implementations.
     * @see JmxMetricExporterSpi
     */
    public MetricExporterSpi[] getMetricExporterSpi() {
        return metricExporterSpi;
    }

    /**
     * Sets metrics registry name for Kafka to Ignite CDC
     *
     * @param name Registry name.
     */
    public void setMetricRegistryName(String name) {
        this.metricRegName = name;
    }

    /**
     * @return Metrics registry name.
     */
    public String getMetricRegistryName() {
        return metricRegName == null ? DFLT_METRICS_REG_NAME : metricRegName;
    }
}
