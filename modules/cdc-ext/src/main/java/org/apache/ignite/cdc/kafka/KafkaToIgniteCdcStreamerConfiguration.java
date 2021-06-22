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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;

/**
 * Configuration of {@link KafkaToIgniteCdcStreamer} application.
 *
 * @see KafkaToIgniteCdcStreamer
 * @see KafkaToIgniteLoader
 */
public class KafkaToIgniteCdcStreamerConfiguration {
    /** Default {@link #kafkaPartsTo} value. */
    public static final int DFLT_PARTS = 16;

    /** Default {@link #topic} value. */
    public static final String DFLT_TOPIC = "ignite";

    /** Default {@link #maxBatchSize} value. */
    public static final int DFLT_MAX_BATCH_SIZE = 1024;

    /** {@link KafkaToIgniteCdcStreamerApplier} thread count. */
    private int threadCnt = DFLT_PARTS;

    /** Topic name. */
    private String topic = DFLT_TOPIC;

    /** Kafka partitions lower bound (inclusive). */
    private int kafkaPartsFrom = 0;

    /** Kafka partitions higher bound (exclusive). */
    private int kafkaPartsTo = DFLT_PARTS;

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

    /** */
    public int getThreadCount() {
        return threadCnt;
    }

    /** */
    public void setThreadCount(int threadCnt) {
        this.threadCnt = threadCnt;
    }

    /** */
    public String getTopic() {
        return topic;
    }

    /** */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /** */
    public int getKafkaPartsFrom() {
        return kafkaPartsFrom;
    }

    /** */
    public void setKafkaPartsFrom(int kafkaPartsFrom) {
        this.kafkaPartsFrom = kafkaPartsFrom;
    }

    /** */
    public int getKafkaPartsTo() {
        return kafkaPartsTo;
    }

    /** */
    public void setKafkaPartsTo(int kafkaPartsTo) {
        this.kafkaPartsTo = kafkaPartsTo;
    }

    /** */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /** */
    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    /** */
    public Collection<String> getCaches() {
        return caches;
    }

    /** */
    public void setCaches(Collection<String> caches) {
        this.caches = caches;
    }
}
