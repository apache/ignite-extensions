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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Thread that polls message from the Kafka topic partitions and applies those messages to the Ignite caches.
 * It expected that messages was written to the Kafka by the {@link IgniteToKafkaCdcStreamer} Change Data Capture consumer.
 * <p>
 * Each {@code Applier} receive set of Kafka topic partitions to read and caches to process.
 * Applier creates consumer per partition because Kafka consumer reads not fair,
 * consumer reads messages from specific partition while there is new messages in specific partition.
 * See <a href=
 * "https://cwiki.apache.org/confluence/display/KAFKA/KIP-387%3A+Fair+Message+Consumption+Across+Partitions+in+KafkaConsumer">KIP-387</a>
 * and <a href="https://issues.apache.org/jira/browse/KAFKA-3932">KAFKA-3932</a> for further information.
 * All consumers should belongs to the same consumer-group to ensure consistent reading.
 * Applier polls messages from each consumer in round-robin fashion.
 * <p>
 * Messages applied to Ignite using {@link IgniteInternalCache#putAllConflict(Map)}, {@link IgniteInternalCache#removeAllConflict(Map)}
 * these methods allows to provide {@link GridCacheVersion} of the entry to the Ignite so in case update conflicts they can be resolved
 * by the {@link CacheVersionConflictResolver}.
 * <p>
 * In case of any error during read applier just fail.
 * Fail of any applier will lead to the fail of {@link KafkaToIgniteCdcStreamer} application.
 * It expected that application will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka or Ignite unavailability.
 *
 * @see KafkaToIgniteCdcStreamer
 * @see IgniteToKafkaCdcStreamer
 * @see IgniteInternalCache#putAllConflict(Map)
 * @see IgniteInternalCache#removeAllConflict(Map)
 * @see CacheVersionConflictResolver
 * @see GridCacheVersion
 * @see ChangeDataCaptureEvent
 * @see CacheEntryVersion
 */
class Applier implements Runnable, AutoCloseable {
    /** */
    public static final int DFLT_REQ_TIMEOUT = 3;

    /** Ignite instance. */
    private final IgniteEx ign;

    /** Log. */
    private final IgniteLogger log;

    /** Closed flag. Shared between all appliers. */
    private final AtomicBoolean closed;

    /** Caches. */
    private final Map<Integer, IgniteInternalCache<BinaryObject, BinaryObject>> ignCaches = new HashMap<>();

    /** Kafka properties. */
    private final Properties kafkaProps;

    /** Maximum batch size. */
    private final int maxBatchSize;

    /** Topic to read. */
    private final String topic;

    /** Lower kafka partition (inclusive). */
    private final int kafkaPartFrom;

    /** Higher kafka partition (exclusive). */
    private final int kafkaPartTo;

    /** Caches ids to read. */
    private final Set<Integer> caches;

    /** Consumers. */
    private final List<KafkaConsumer<Integer, byte[]>> cnsmrs = new ArrayList<>();

    /** Update batch. */
    private final Map<KeyCacheObject, GridCacheDrInfo> updBatch = new HashMap<>();

    /** Remove batch. */
    private final Map<KeyCacheObject, GridCacheVersion> rmvBatch = new HashMap<>();

    /** */
    private final AtomicLong rcvdEvts = new AtomicLong();

    /**
     * @param ign Ignite instance.
     * @param log Logger.
     * @param kafkaProps Kafka properties.
     * @param topic Topic name.
     * @param kafkaPartFrom Read from partition.
     * @param kafkaPartTo Read to partition.
     * @param caches Cache ids.
     * @param maxBatchSize Maximum batch size.
     * @param closed Closed flag.
     */
    public Applier(
        IgniteEx ign,
        IgniteLogger log,
        Properties kafkaProps,
        String topic,
        int kafkaPartFrom,
        int kafkaPartTo,
        Set<Integer> caches,
        int maxBatchSize,
        AtomicBoolean closed
    ) {
        this.ign = ign;
        this.kafkaProps = kafkaProps;
        this.topic = topic;
        this.kafkaPartFrom = kafkaPartFrom;
        this.kafkaPartTo = kafkaPartTo;
        this.caches = caches;
        this.maxBatchSize = maxBatchSize;
        this.closed = closed;
        this.log = log.getLogger(Applier.class);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        U.setCurrentIgniteName(ign.name());

        try {
            for (int kafkaPart = kafkaPartFrom; kafkaPart < kafkaPartTo; kafkaPart++) {
                KafkaConsumer<Integer, byte[]> cnsmr = new KafkaConsumer<>(kafkaProps);

                cnsmr.assign(Collections.singleton(new TopicPartition(topic, kafkaPart)));

                cnsmrs.add(cnsmr);
            }

            Iterator<KafkaConsumer<Integer, byte[]>> cnsmrIter = Collections.emptyIterator();

            while (!closed.get()) {
                if (!cnsmrIter.hasNext())
                    cnsmrIter = cnsmrs.iterator();

                poll(cnsmrIter.next());
            }
        }
        catch (WakeupException e) {
            if (!closed.get())
                log.error("Applier wakeup error!", e);
        }
        catch (Throwable e) {
            log.error("Applier error!", e);

            closed.set(true);
        }
        finally {
            for (KafkaConsumer<Integer, byte[]> consumer : cnsmrs) {
                try {
                    consumer.close(Duration.ofSeconds(DFLT_REQ_TIMEOUT));
                }
                catch (Exception e) {
                    log.warning("Close error!", e);
                }
            }

            cnsmrs.clear();
        }

        if (log.isInfoEnabled())
            log.info(Thread.currentThread().getName() + " - stopped!");
    }

    /**
     * Polls data from the specific consumer and applies it to the Ignite.
     * @param cnsmr Data consumer.
     */
    private void poll(KafkaConsumer<Integer, byte[]> cnsmr) throws IgniteCheckedException {
        ConsumerRecords<Integer, byte[]> records = cnsmr.poll(Duration.ofSeconds(DFLT_REQ_TIMEOUT));

        if (log.isDebugEnabled()) {
            log.debug(
                "Polled from consumer [assignments=" + cnsmr.assignment() + ",rcvdEvts=" + rcvdEvts.addAndGet(records.count()) + ']'
            );
        }

        IgniteInternalCache<BinaryObject, BinaryObject> currCache = null;

        for (ConsumerRecord<Integer, byte[]> rec : records) {
            if (!F.isEmpty(caches) && !caches.contains(rec.key()))
                continue;

            try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(rec.value()))) {
                ChangeDataCaptureEvent evt = (ChangeDataCaptureEvent)is.readObject();

                IgniteInternalCache<BinaryObject, BinaryObject> cache = ignCaches.computeIfAbsent(evt.cacheId(), cacheId -> {
                    for (String cacheName : ign.cacheNames()) {
                        if (CU.cacheId(cacheName) == cacheId)
                            return ign.cachex(cacheName);
                    }

                    throw new IllegalStateException("Cache with id not found [cacheId=" + cacheId + ']');
                });

                if (cache != currCache) {
                    applyIfNotEmpty(currCache);

                    updBatch.clear();
                    rmvBatch.clear();

                    currCache = cache;
                }

                CacheEntryVersion order = evt.version();

                KeyCacheObject key = new KeyCacheObjectImpl(evt.key(), null, evt.partition());

                if (evt.value() != null) {
                    applyIfRequired(currCache, key, true);

                    CacheObject val = new CacheObjectImpl(evt.value(), null);

                    updBatch.put(key, new GridCacheDrInfo(val,
                        new GridCacheVersion(order.topologyVersion(), order.order(), order.nodeOrder(), order.clusterId())));
                }
                else {
                    applyIfRequired(currCache, key, false);

                    rmvBatch.put(key,
                        new GridCacheVersion(order.topologyVersion(), order.order(), order.nodeOrder(), order.clusterId()));
                }
            }
            catch (ClassNotFoundException | IOException e) {
                throw new IgniteCheckedException(e);
            }
        }

        if (currCache != null)
            applyIfNotEmpty(currCache);

        cnsmr.commitSync(Duration.ofSeconds(DFLT_REQ_TIMEOUT));
    }

    /**
     * Applies data from {@code updMap} or {@code rmvBatch} to Ignite if required.
     *
     * @param cache Current cache.
     * @throws IgniteCheckedException In case of error.
     */
    private void applyIfNotEmpty(IgniteInternalCache<BinaryObject, BinaryObject> cache) throws IgniteCheckedException {
        if (!F.isEmpty(rmvBatch)) {
            cache.removeAllConflict(rmvBatch);

            rmvBatch.clear();
        }

        if (!F.isEmpty(updBatch)) {
            cache.putAllConflict(updBatch);

            updBatch.clear();
        }
    }

    /**
     * Applies data from {@code updMap} or {@code rmvBatch} to Ignite if required.
     *
     * @param cache Current cache.
     * @param key Key.
     * @param isUpdate {@code True} if next operation update.
     * @throws IgniteCheckedException In case of error.
     */
    private void applyIfRequired(
        IgniteInternalCache<BinaryObject, BinaryObject> cache,
        KeyCacheObject key,
        boolean isUpdate
    ) throws IgniteCheckedException {
        if (isApplyBatch(false, rmvBatch, isUpdate, key)) {
            cache.removeAllConflict(rmvBatch);

            rmvBatch.clear();
        }

        if (isApplyBatch(true, updBatch, isUpdate, key)) {
            cache.putAllConflict(updBatch);

            updBatch.clear();
        }
    }

    /** @return {@code True} if update batch should be applied. */
    private boolean isApplyBatch(
        boolean batchContainsUpd,
        Map<KeyCacheObject, ?> map,
        boolean currOpUpd,
        KeyCacheObject key
    ) {
        return (!F.isEmpty(map) && currOpUpd != batchContainsUpd) ||
            map.size() >= maxBatchSize ||
            map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.warning("Close applier!");

        closed.set(true);

        cnsmrs.forEach(KafkaConsumer::wakeup);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Applier.class, this);
    }
}
