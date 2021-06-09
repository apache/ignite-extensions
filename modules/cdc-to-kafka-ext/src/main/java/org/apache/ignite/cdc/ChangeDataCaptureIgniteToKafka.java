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

package org.apache.ignite.cdc;

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
import org.apache.ignite.cdc.conflictplugin.CacheVersionConflictResolverImpl;
import org.apache.ignite.internal.cdc.ChangeDataCapture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.LoggerResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Change Data Consumer that streams all data changes to Kafka topic.
 * {@link ChangeDataCaptureEvent} spread across Kafka topic partitions with {@code {ignite_partition} % {kafka_topic_count}} formula.
 * Consumer will just fail in case of any error during write. Fail of consumer will lead to the fail of {@code ignite-cdc} application.
 * It expected that {@code ignite-cdc} will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka unavailability or network issues.
 *
 * If you have plans to apply written messages to the other Ignite cluster in active-active manner,
 * e.g. concurrent updates of the same entry in other cluster is possible,
 * please, be aware of {@link CacheVersionConflictResolverImpl} conflict resolved.
 * Configuration of {@link CacheVersionConflictResolverImpl} can be found in {@link ChangeDataCaptureKafkaToIgnite} documentation.
 *
 * @see ChangeDataCapture
 * @see ChangeDataCaptureKafkaToIgnite
 * @see CacheVersionConflictResolverImpl
 */
public class ChangeDataCaptureIgniteToKafka implements ChangeDataCaptureConsumer {
    /** Log. */
    @LoggerResource
    private IgniteLogger log;

    /** Kafka producer to stream events. */
    private KafkaProducer<Integer, ChangeDataCaptureEvent> producer;

    /** Handle only primary entry flag. */
    private final boolean onlyPrimary;

    /** Topic name. */
    private final String topic;

    /** Kafka topic partitions count. */
    private int kafkaPartsCnt;

    /** Cache IDs. */
    private final Set<Integer> cachesIds;

    /** Max batch size. */
    private final int maxBatchSize;

    /** Kafka properties. */
    private final Properties kafkaProps;

    /** Count of sent messages.  */
    private long msgCnt;

    /**
     * @param topic Topic name.
     * @param caches Cache names.
     * @param maxBatchSize Maximum count of concurrently.
     * @param onlyPrimary If {@code true} then stream only events from primaries.
     * @param kafkaProps Kafka properties.
     */
    public ChangeDataCaptureIgniteToKafka(String topic, Set<String> caches, int maxBatchSize, boolean onlyPrimary, Properties kafkaProps) {
        assert caches != null && !caches.isEmpty();

        this.topic = topic;
        this.onlyPrimary = onlyPrimary;
        this.kafkaProps = kafkaProps;
        this.maxBatchSize = maxBatchSize;

        cachesIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<ChangeDataCaptureEvent> evts) {
        List<Future<RecordMetadata>> futs = new ArrayList<>();

        while (evts.hasNext() && futs.size() <= maxBatchSize) {
            ChangeDataCaptureEvent evt = evts.next();

            if (onlyPrimary && !evt.primary())
                continue;

            if (evt.version().otherClusterVersion() != null)
                continue;

            if (!cachesIds.isEmpty() && !cachesIds.contains(evt.cacheId()))
                continue;

            msgCnt++;

            futs.add(producer.send(new ProducerRecord<>(
                topic,
                evt.partition() % kafkaPartsCnt,
                evt.cacheId(),
                evt
            )));
        }

        try {
            for (Future<RecordMetadata> fut : futs)
                fut.get(KafkaUtils.DFLT_REQ_TIMEOUT_MIN, TimeUnit.MINUTES);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        if (log.isDebugEnabled())
            log.debug("Events processed [sentMessagesCount=" + msgCnt + ']');

        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        try {
            kafkaPartsCnt = KafkaUtils.initTopic(topic, kafkaProps);

            producer = new KafkaProducer<>(kafkaProps);

            log.info("CDC Ignite To Kafka started [topic=" + topic + ", onlyPrimary=" + onlyPrimary + ", cacheIds=" + cachesIds + ']');
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        producer.close();
    }
}
