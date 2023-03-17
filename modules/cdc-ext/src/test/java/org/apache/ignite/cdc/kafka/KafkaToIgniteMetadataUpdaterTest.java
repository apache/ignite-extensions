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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cdc.AbstractCdcEventsApplier;
import org.apache.ignite.cdc.thin.CdcEventsIgniteClientApplier;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.BinaryNoopMetadataHandler;
import org.apache.ignite.internal.cdc.TypeMappingImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.Test;

import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.META_UPDATE_MARKER;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 *
 */
public class KafkaToIgniteMetadataUpdaterTest extends GridCommonAbstractTest {
    /** Event topic. */
    private static final String EVT_TOPIC = "evt-topic";

    /** Meta topic. */
    private static final String META_TOPIC = "meta-topic";

    /** Parts count. One thread per partition. */
    public static final int PARTS_CNT = 5;

    /** Kafka request timeout. */
    public static final int KAFKA_REQ_TIMEOUT = 5000;

    /** Test metadata. */
    public static final BinaryMetadata TEST_METADATA = new BinaryMetadata(10, "testMetadata", null,
        null, null, false, null);

    /** Test mapping. */
    public static final TypeMappingImpl TEST_MAPPING = new TypeMappingImpl(12, "testMapping", PlatformType.JAVA);

    /** Kafka cluster. */
    private EmbeddedKafkaCluster kafkaCluster;

    /** */
    public KafkaToIgniteMetadataUpdaterTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (kafkaCluster == null) {
            kafkaCluster = new EmbeddedKafkaCluster(1);

            kafkaCluster.start();
        }

        kafkaCluster.createTopic(EVT_TOPIC, PARTS_CNT, 1);
        kafkaCluster.createTopic(META_TOPIC, 1, 1);

        waitForCondition(() -> kafkaCluster.getAllTopicsInCluster().containsAll(F.asList(META_TOPIC, EVT_TOPIC)),
            getTestTimeout());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String s : kafkaCluster.getAllTopicsInCluster())
            kafkaCluster.deleteTopic(s);

        waitForCondition(() -> kafkaCluster.getAllTopicsInCluster().isEmpty(), getTestTimeout());
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /** */
    @Test
    public void testMultiThreadedMetadataUpdate() throws Exception {
        TestBinaryContext binCtx = new TestBinaryContext(BinaryNoopMetadataHandler.instance(),
            new IgniteConfiguration(), log);

        CountDownLatch metaLatch = new CountDownLatch(PARTS_CNT);

        List<IgniteInternalFuture<?>> applierFuts = runAppliers(binCtx, PARTS_CNT, streamerConfiguration(), metaLatch);

        try (KafkaProducer<Void, byte[]> producer = new KafkaProducer<>(producerProperties())) {
            sendMetadataAndMarkers(producer, PARTS_CNT);

            // All appliers should process metadata update markers faster than KAFKA_REQ_TIMEOUT * PARTS_CNT.
            assertTrue("Appliers has not updated metadata",
                metaLatch.await(KAFKA_REQ_TIMEOUT * (PARTS_CNT - 1), TimeUnit.MILLISECONDS));

            for (IgniteInternalFuture<?> fut : applierFuts)
                assertFalse("Future finished: err=" + X.getFullStackTrace(fut.error()), fut.isDone());

            assertEquals("Unexpected binary metadata count", 1, binCtx.types.size());
            assertEquals("Unexpected type mappings count", 1, binCtx.mappings.size());

            BinaryMetadata binaryMeta = binCtx.types.get(0);
            assertEquals("Unexpected 'typeId'", 10, binaryMeta.typeId());
            assertEquals("Unexpected 'typeName'", "testMetadata", binaryMeta.typeName());

            String clsName = binCtx.mappings.get(12);
            assertNotNull("Expected mapping nod found", clsName);
            assertEquals("Unexpected 'clsName'", "testMapping", clsName);
        }
        finally {
            for (IgniteInternalFuture<?> fut : applierFuts)
                fut.cancel();
        }
    }

    /** */
    private void sendMetadataAndMarkers(KafkaProducer<Void, byte[]> producer, int partsCnt) throws Exception {
        List<ProducerRecord<Void, byte[]>> recs = new ArrayList<>();

        recs.add(new ProducerRecord<>(META_TOPIC, U.toBytes(TEST_METADATA)));
        recs.add(new ProducerRecord<>(META_TOPIC, U.toBytes(TEST_MAPPING)));

        for (int p = 0; p < partsCnt; p++)
            recs.add(new ProducerRecord<>(EVT_TOPIC, p, null, META_UPDATE_MARKER));

        for (ProducerRecord<Void, byte[]> rec : recs)
            producer.send(rec).get(KAFKA_REQ_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /** */
    private List<IgniteInternalFuture<?>> runAppliers(BinaryContext binCtx, int appliersCnt,
        KafkaToIgniteCdcStreamerConfiguration cfg, CountDownLatch metaLatch) {
        AtomicBoolean stopped = new AtomicBoolean();

        KafkaToIgniteMetadataUpdater metadataUpdater = new KafkaToIgniteMetadataUpdater(binCtx, log, consumerProperties(), cfg);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        // Create one applier per partition, i.e. one thread per partition.
        for (int i = 0; i < appliersCnt; i++) {
            TestKafkaToIgniteCdcStreamerApplier streamerApplier = new TestKafkaToIgniteCdcStreamerApplier(
                // Dummy applier, client won't be called:
                () -> new CdcEventsIgniteClientApplier(null, 0, log),
                log,
                consumerProperties(),
                cfg.getTopic(),
                i,
                i + 1,
                Collections.emptySet(),
                0,
                cfg.getKafkaRequestTimeout(),
                metadataUpdater,
                stopped);

            streamerApplier.setMetaLatch(metaLatch);

            futs.add(runAsync(streamerApplier));
        }

        return futs;
    }

    /** */
    private Properties consumerProperties() {
        Properties props = new Properties();

        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
        props.put(GROUP_ID_CONFIG, "test-group");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return props;
    }

    /** */
    private Properties producerProperties() {
        Properties props = new Properties();

        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
        props.put(KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return props;
    }

    /** */
    private static KafkaToIgniteCdcStreamerConfiguration streamerConfiguration() {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();
        cfg.setTopic(EVT_TOPIC);
        cfg.setMetadataTopic(META_TOPIC);
        cfg.setKafkaPartsFrom(0);
        cfg.setKafkaPartsTo(PARTS_CNT);
        cfg.setKafkaRequestTimeout(KAFKA_REQ_TIMEOUT);

        return cfg;
    }

    /** */
    private static class TestKafkaToIgniteCdcStreamerApplier extends KafkaToIgniteCdcStreamerApplier {
        /** Metadata latch. Is counted down after metadata update call performed. */
        private CountDownLatch metaLatch;

        /** */
        public TestKafkaToIgniteCdcStreamerApplier(Supplier<AbstractCdcEventsApplier> applierSupplier,
            IgniteLogger log, Properties kafkaProps, String topic, int kafkaPartFrom, int kafkaPartTo,
            Set<Integer> caches, int maxBatchSize, long kafkaReqTimeout, KafkaToIgniteMetadataUpdater metaUpdr,
            AtomicBoolean stopped) {
            super(applierSupplier, log, kafkaProps, topic, kafkaPartFrom, kafkaPartTo, caches, maxBatchSize,
                kafkaReqTimeout, metaUpdr, stopped);
        }

        /** {@inheritDoc} */
        @Override protected boolean filterAndPossiblyUpdateMetadata(ConsumerRecord<Integer, byte[]> rec) {
            boolean retVal = super.filterAndPossiblyUpdateMetadata(rec);

            // retVal will be 'false' if metadata update has been performed in super method.
            if (!retVal) {
                metaLatch.countDown();

                log.warning(">>>>>> Meta update latch has been counted down.");
            }

            return retVal;
        }

        /** */
        public void setMetaLatch(CountDownLatch metaLatch) {
            this.metaLatch = metaLatch;
        }
    }

    /** */
    private static class TestBinaryContext extends BinaryContext {
        /** Registered types. */
        private final List<BinaryMetadata> types = new CopyOnWriteArrayList<>();

        /** Registered mappings. */
        private final Map<Integer, String> mappings = new ConcurrentHashMap<>();

        /** */
        public TestBinaryContext(BinaryMetadataHandler metaHnd, IgniteConfiguration igniteCfg, IgniteLogger log) {
            super(metaHnd, igniteCfg, log);
        }

        /** {@inheritDoc} */
        @Override public void updateMetadata(int typeId, BinaryMetadata meta, boolean failIfUnregistered)
            throws BinaryObjectException {
            types.add(meta);

            log.warning(">>>>>> Registered binary metadata with typeId=" + typeId);
        }

        /** {@inheritDoc} */
        @Override public boolean registerUserClassName(int typeId, String clsName, boolean failIfUnregistered, boolean onlyLocReg,
            byte platformId) {
            mappings.put(typeId, clsName);

            log.warning(">>>>>> Registered class with typeId=" + typeId);

            return true;
        }
    }
}
