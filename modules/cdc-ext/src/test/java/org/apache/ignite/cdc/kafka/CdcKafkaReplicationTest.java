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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import kafka.security.authorizer.AclEntry$;
import org.apache.ignite.cdc.AbstractReplicationTest;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.IgniteToIgniteCdcStreamer;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

/**
 * Tests for kafka replication.
 */
public class CdcKafkaReplicationTest extends AbstractReplicationTest {
    /** */
    public static final String SRC_DEST_TOPIC = "source-dest";

    /** */
    public static final String DEST_SRC_TOPIC = "dest-source";

    /** */
    public static final String SRC_DEST_META_TOPIC = "source-dest-meta";

    /** */
    public static final String DEST_SRC_META_TOPIC = "dest-source-meta";

    /** */
    public static final int DFLT_PARTS = 16;

    /** JAAS prefix for authentication configuration. */
    public static final String JAAS_PREFIX = PlainLoginModule.class.getName() + " required";

    /** Kafka principal string for client. */
    public static final String CLIENT_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client").toString();

    /** Write permission for client. */
    public static final AccessControlEntry CLIENT_ALLOW_WRITE =
        new AccessControlEntry(CLIENT_PRINCIPAL, AclEntry$.MODULE$.WildcardHost(), WRITE, ALLOW);

    /** Read permission for client. */
    public static final AccessControlEntry CLIENT_ALLOW_READ =
        new AccessControlEntry(CLIENT_PRINCIPAL, AclEntry$.MODULE$.WildcardHost(), READ, ALLOW);

    /** */
    private static EmbeddedKafkaCluster KAFKA = null;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (KAFKA == null) {
            KAFKA = new EmbeddedKafkaCluster(1, kafkaBrokerConfig());

            KAFKA.start();
        }

        withAdminClient(adminClient -> createTopicsAndAcls(adminClient, SRC_DEST_TOPIC, DEST_SRC_TOPIC,
            SRC_DEST_META_TOPIC, DEST_SRC_META_TOPIC));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        withAdminClient(this::awaitDeleteTopics);
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActivePassiveCdc(String cache) {
        withAdminClient(adminClient -> createTopicsAndAcls(adminClient, cache));

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (IgniteEx ex : srcCluster)
            futs.add(igniteToKafka(ex.configuration(), cache, SRC_DEST_META_TOPIC, cache));

        for (int i = 0; i < destCluster.length; i++) {
            futs.add(kafkaToIgnite(
                cache,
                cache,
                SRC_DEST_META_TOPIC,
                destClusterCliCfg[i],
                destCluster,
                i * (DFLT_PARTS / 2),
                (i + 1) * (DFLT_PARTS / 2)
            ));
        }

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActiveActiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (IgniteEx ex : srcCluster)
            futs.add(igniteToKafka(ex.configuration(), SRC_DEST_TOPIC, SRC_DEST_META_TOPIC, ACTIVE_ACTIVE_CACHE));

        for (IgniteEx ex : destCluster)
            futs.add(igniteToKafka(ex.configuration(), DEST_SRC_TOPIC, DEST_SRC_META_TOPIC, ACTIVE_ACTIVE_CACHE));

        futs.add(kafkaToIgnite(
            ACTIVE_ACTIVE_CACHE,
            SRC_DEST_TOPIC,
            SRC_DEST_META_TOPIC,
            destClusterCliCfg[0],
            destCluster,
            0,
            DFLT_PARTS
        ));

        futs.add(kafkaToIgnite(
            ACTIVE_ACTIVE_CACHE,
            DEST_SRC_TOPIC,
            DEST_SRC_META_TOPIC,
            srcClusterCliCfg[0],
            srcCluster,
            0,
            DFLT_PARTS
        ));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected void checkConsumerMetrics(Function<String, Long> longMetric) {
        assertNotNull(longMetric.apply(IgniteToIgniteCdcStreamer.LAST_EVT_TIME));
        assertNotNull(longMetric.apply(IgniteToIgniteCdcStreamer.EVTS_CNT));
        assertNotNull(longMetric.apply(IgniteToKafkaCdcStreamer.BYTES_SENT));
    }

    /**
     * @param igniteCfg Ignite configuration.
     * @param topic Kafka topic name.
     * @param metadataTopic Metadata topic name.
     * @param cache Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> igniteToKafka(
        IgniteConfiguration igniteCfg,
        String topic,
        String metadataTopic,
        String cache
    ) {
        return runAsync(() -> {
            IgniteToKafkaCdcStreamer cdcCnsmr = new IgniteToKafkaCdcStreamer()
                .setTopic(topic)
                .setMetadataTopic(metadataTopic)
                .setKafkaPartitions(DFLT_PARTS)
                .setCaches(Collections.singleton(cache))
                .setMaxBatchSize(KEYS_CNT)
                .setOnlyPrimary(false)
                .setKafkaProperties(clientProperties())
                .setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

            CdcConfiguration cdcCfg = new CdcConfiguration();

            cdcCfg.setConsumer(cdcCnsmr);
            cdcCfg.setMetricExporterSpi(new JmxMetricExporterSpi());

            CdcMain cdc = new CdcMain(igniteCfg, null, cdcCfg);

            cdcs.add(cdc);

            cdc.run();
        });
    }

    /**
     * @param cacheName Cache name.
     * @param igniteCfg Ignite configuration.
     * @param dest Destination Ignite cluster.
     * @return Future for runed {@link KafkaToIgniteCdcStreamer}.
     */
    protected IgniteInternalFuture<?> kafkaToIgnite(
        String cacheName,
        String topic,
        String metadataTopic,
        IgniteConfiguration igniteCfg,
        IgniteEx[] dest,
        int fromPart,
        int toPart
    ) {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();

        cfg.setKafkaPartsFrom(fromPart);
        cfg.setKafkaPartsTo(toPart);
        cfg.setThreadCount((toPart - fromPart) / 2);

        cfg.setCaches(Collections.singletonList(cacheName));
        cfg.setTopic(topic);
        cfg.setMetadataTopic(metadataTopic);
        cfg.setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

        if (clientType == ClientType.THIN_CLIENT) {
            ClientConfiguration clientCfg = new ClientConfiguration();

            clientCfg.setAddresses(hostAddresses(dest));

            return runAsync(new KafkaToIgniteClientCdcStreamer(clientCfg, clientProperties(), cfg));
        }
        else
            return runAsync(new KafkaToIgniteCdcStreamer(igniteCfg, clientProperties(), cfg));
    }

    /** */
    protected Properties clientProperties() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        // Authentication.
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
        props.put("sasl.jaas.config", JAAS_PREFIX + " username=\"client\" password=\"client_password\";");

        return props;
    }

    /** */
    protected Properties adminClientProperties() {
        Properties props = new Properties();

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        // Authentication.
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.jaas.config", JAAS_PREFIX + " username=\"admin\" password=\"admin_password\";");

        return props;
    }

    /** */
    protected Properties kafkaBrokerConfig() {
        String brokerCredentials = "username=\"admin\" password=\"admin_password\"";
        String adminCredentials = "user_admin=\"admin_password\"";
        String clientCredentials = "user_client=\"client_password\"";

        String jaasSrvCfg = String.format("%s %s %s %s;", JAAS_PREFIX, brokerCredentials, adminCredentials,
            clientCredentials);

        Properties brokerCfg = new Properties();

        // Listeners.
        brokerCfg.put("listeners", "SASL_PLAINTEXT://localhost:9092");
        brokerCfg.put("sasl.enabled.mechanisms", "PLAIN");
        brokerCfg.put("sasl.mechanism.inter.broker.protocol", "PLAIN");

        // Property "security.inter.broker.protocol" is not used, because it is mutually exclusive to
        // the below "inter.broker.listener.name" one, which in turn is used in KafkaEmbedded#brokerList
        // to determine listener ports.
        brokerCfg.put("inter.broker.listener.name", "SASL_PLAINTEXT");

        // Authentication.
        brokerCfg.put("listener.name.sasl_plaintext.plain.sasl.jaas.config", jaasSrvCfg);

        // Authorization.
        brokerCfg.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
        brokerCfg.put("super.users", "User:admin");

        return brokerCfg;
    }

    /**
     * Perform action with admin client.
     */
    protected void withAdminClient(ConsumerX<AdminClient> adminAction) {
        try (AdminClient adminClient = KafkaAdminClient.create(adminClientProperties())) {
            adminAction.accept(adminClient);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private void createTopicsAndAcls(AdminClient adminClient, String... topicNames) throws Exception {
        List<NewTopic> newTopics = Arrays.stream(topicNames)
            .map(t -> new NewTopic(t, DFLT_PARTS, (short)1))
            .collect(Collectors.toList());

        adminClient.createTopics(newTopics)
            .all()
            .get(getTestTimeout(), TimeUnit.MILLISECONDS);

        List<AclBinding> aclBindings = new ArrayList<>();

        for (String topic : topicNames) {
            ResourcePattern rsrcPattern = new ResourcePattern(TOPIC, topic, LITERAL);

            AclBinding consumerTopicReadAcl = new AclBinding(rsrcPattern, CLIENT_ALLOW_READ);
            aclBindings.add(consumerTopicReadAcl);

            AclBinding producerTopicWrite = new AclBinding(rsrcPattern, CLIENT_ALLOW_WRITE);
            aclBindings.add(producerTopicWrite);
        }

        String kafkaIgniteStreamerGrp = clientProperties().getProperty(ConsumerConfig.GROUP_ID_CONFIG);

        AclBinding consumerStreamerGrpRead = new AclBinding(
            new ResourcePattern(GROUP, kafkaIgniteStreamerGrp, LITERAL),
            CLIENT_ALLOW_READ);

        AclBinding consumerMetadataGrpReadAcl = new AclBinding(
            new ResourcePattern(GROUP, "ignite-metadata-update", PREFIXED),
            CLIENT_ALLOW_READ);

        // Group read is necessary to commit offsets
        aclBindings.add(consumerStreamerGrpRead);
        aclBindings.add(consumerMetadataGrpReadAcl);

        CreateAclsResult createAclsResult = adminClient.createAcls(aclBindings);

        createAclsResult.all().get(getTestTimeout(), TimeUnit.MILLISECONDS);
    }

    /** */
    private void awaitDeleteTopics(AdminClient adminClient) throws Exception {
        adminClient.deleteAcls(Collections.singleton(AclBindingFilter.ANY)).all().get();

        Set<String> topics = adminClient.listTopics().names().get();

        adminClient.deleteTopics(topics)
            .all()
            .get(getTestTimeout(), TimeUnit.MILLISECONDS);

        boolean noTopics = adminClient.listTopics()
            .names()
            .get()
            .isEmpty();

        assertTrue("Not all topics deleted", noTopics);
    }

    /**
     * Consumer which can throw exception.
     */
    @FunctionalInterface
    private interface ConsumerX<T> {
        /** */
        void accept(T t) throws Exception;
    }
}
