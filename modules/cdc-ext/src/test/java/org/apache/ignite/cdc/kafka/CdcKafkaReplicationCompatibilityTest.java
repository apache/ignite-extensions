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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAbstractTest.DEST_SRC_META_TOPIC;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAbstractTest.DEST_SRC_TOPIC;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAbstractTest.DFLT_PARTS;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAbstractTest.SRC_DEST_META_TOPIC;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAbstractTest.SRC_DEST_TOPIC;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAbstractTest.initKafka;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationAbstractTest.removeKafkaTopicsAndWait;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_18_0;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests active-active CDC replication through Kafka between clusters of different Ignite versions.
 * <p>
 * The old version cluster, its CDC application ({@link IgniteToKafkaCdcStreamer}) and its applier
 * ({@link KafkaToIgniteClientCdcStreamer}) are started in a separate JVM with the old Ignite version in the classpath.
 * The same for the current version cluster in the test JVM.
 */
@RunWith(Parameterized.class)
public class CdcKafkaReplicationCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String CACHE = "replicated-cache";

    /** */
    private static final String VAL_TYPE = "TestValue";

    /** Count of the keys written to each cluster. */
    private static final int KEYS_CNT = 50;

    /** */
    private static final int OLD_DISCO_PORT = 47500;

    /** */
    private static final int CUR_DISCO_PORT = 47600;

    /** */
    private static final int OLD_CLI_PORT = 10800;

    /** */
    private static final int CUR_CLI_PORT = 10900;

    /** */
    private static final byte OLD_CLUSTER_ID = 1;

    /** */
    private static final byte CUR_CLUSTER_ID = 2;

    /** Timeout to wait for the replication. */
    private static final long REPLICATION_TIMEOUT = 60_000;

    /** */
    private static EmbeddedKafkaCluster kafka;

    /** Threads of the replication applications of the current version cluster. */
    private List<Thread> curApps;

    /** Old Ignite version. */
    @Parameterized.Parameter
    public String oldVer;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "oldVer={0}")
    public static Collection<String> versions() {
        return IgniteReleasedVersion.since(VER_2_18_0);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        kafka = initKafka(kafka);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (curApps != null) {
            curApps.forEach(Thread::interrupt);

            for (Thread app : curApps)
                app.join(getTestTimeout());
        }

        stopAllGrids();

        removeKafkaTopicsAndWait(kafka, getTestTimeout());

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testActiveActiveReplication() throws Exception {
        String kafkaAddrs = kafka.bootstrapServers();

        // Old version cluster: source for the SRC_DEST topics, destination for the DEST_SRC topics.
        startGrid(
            1,
            oldVer.toString(),
            cfg -> configure(cfg, "old", OLD_DISCO_PORT, OLD_CLI_PORT, OLD_CLUSTER_ID),
            ign -> startCacheAndReplication(
                ign, kafkaAddrs, SRC_DEST_TOPIC, SRC_DEST_META_TOPIC, DEST_SRC_TOPIC, DEST_SRC_META_TOPIC, OLD_CLI_PORT)
        );

        // Current version cluster. The node is started directly, because it must not join the old version one.
        Ignite cur = Ignition.start(
            configure(getConfiguration(getTestIgniteInstanceName(0)), "current", CUR_DISCO_PORT, CUR_CLI_PORT, CUR_CLUSTER_ID));

        curApps = startCacheAndReplication(
            cur, kafkaAddrs, DEST_SRC_TOPIC, DEST_SRC_META_TOPIC, SRC_DEST_TOPIC, SRC_DEST_META_TOPIC, CUR_CLI_PORT);

        try (IgniteClient oldCli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + OLD_CLI_PORT))) {
            ClientCache<Integer, BinaryObject> oldCache = oldCli.<Integer, Object>cache(CACHE).withKeepBinary();
            IgniteCache<Integer, BinaryObject> curCache = cur.<Integer, Object>cache(CACHE).withKeepBinary();

            for (int i = 0; i < KEYS_CNT; i++)
                oldCache.put(i, oldCli.binary().builder(VAL_TYPE).setField("id", i).setField("name", "old-" + i).build());

            for (int i = KEYS_CNT; i < 2 * KEYS_CNT; i++)
                curCache.put(i, cur.binary().builder(VAL_TYPE).setField("id", i).setField("name", "current-" + i).build());

            assertTrue("Data must be replicated to both clusters", waitForCondition(() -> {
                for (int i = 0; i < 2 * KEYS_CNT; i++) {
                    String exp = (i < KEYS_CNT ? "old-" : "current-") + i;

                    if (!hasValue(oldCache.get(i), i, exp) || !hasValue(curCache.get(i), i, exp))
                        return false;
                }

                return true;
            }, REPLICATION_TIMEOUT));

            assertEquals(2 * KEYS_CNT, oldCache.size());
            assertEquals(2 * KEYS_CNT, curCache.size());
        }
    }

    /** */
    private static boolean hasValue(BinaryObject val, int id, String name) {
        return val != null && Integer.valueOf(id).equals(val.field("id")) && name.equals(val.field("name"));
    }

    /**
     * Note, the method is executed both by the current and by the old Ignite version.
     *
     * @param cfg Configuration to tune.
     * @param consId Consistent id.
     * @param discoPort Discovery port, each cluster has its own.
     * @param cliPort Thin client port.
     * @param clusterId Cluster id for the conflict resolver.
     * @return Tuned configuration.
     */
    private static IgniteConfiguration configure(IgniteConfiguration cfg, String consId, int discoPort, int cliPort, byte clusterId) {
        CacheVersionConflictResolverPluginProvider<?> conflictPlugin = new CacheVersionConflictResolverPluginProvider<>();

        conflictPlugin.setClusterId(clusterId);
        conflictPlugin.setCaches(Collections.singleton(CACHE));

        return cfg
            .setConsistentId(consId)
            .setLocalHost("127.0.0.1")
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(discoPort)
                .setLocalPortRange(1)
                .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Collections.singleton("127.0.0.1:" + discoPort))))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setPort(cliPort).setPortRange(0))
            .setPluginProviders(conflictPlugin)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalForceArchiveTimeout(1_000)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setCdcEnabled(true)));
    }

    /**
     * Creates the cache and starts both directions of the replication for the node:
     * CDC application that streams changes of the node to Kafka and applier of the changes of another cluster.
     * Note, the method is executed both by the current and by the old Ignite version.
     *
     * @param ign Node.
     * @param kafkaAddrs Kafka bootstrap servers.
     * @param outTopic Topic to stream changes of the node to.
     * @param outMetaTopic Topic to stream metadata of the node to.
     * @param inTopic Topic with changes of another cluster.
     * @param inMetaTopic Topic with metadata of another cluster.
     * @param cliPort Thin client port of the node.
     * @return Threads of the started applications.
     */
    private static List<Thread> startCacheAndReplication(
        Ignite ign,
        String kafkaAddrs,
        String outTopic,
        String outMetaTopic,
        String inTopic,
        String inMetaTopic,
        int cliPort
    ) {
        ign.cluster().state(ClusterState.ACTIVE);

        ign.getOrCreateCache(CACHE);

        IgniteToKafkaCdcStreamer cdcCnsmr = new IgniteToKafkaCdcStreamer()
            .setTopic(outTopic)
            .setMetadataTopic(outMetaTopic)
            .setKafkaPartitions(DFLT_PARTS)
            .setCaches(Collections.singleton(CACHE))
            .setKafkaProperties(kafkaProperties(kafkaAddrs, outTopic));

        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(cdcCnsmr);

        Thread cdc = runDaemon(new CdcMain(ign.configuration(), null, cdcCfg), "ignite-to-kafka-" + outTopic);

        KafkaToIgniteCdcStreamerConfiguration applierCfg = new KafkaToIgniteCdcStreamerConfiguration();

        applierCfg.setKafkaPartsFrom(0);
        applierCfg.setKafkaPartsTo(DFLT_PARTS);
        applierCfg.setThreadCount(2);
        applierCfg.setCaches(Collections.singletonList(CACHE));
        applierCfg.setTopic(inTopic);
        applierCfg.setMetadataTopic(inMetaTopic);
        applierCfg.setMetricRegistryName("applier-" + inTopic);

        Thread applier = runDaemon(
            new KafkaToIgniteClientCdcStreamer(
                new ClientConfiguration().setAddresses("127.0.0.1:" + cliPort),
                kafkaProperties(kafkaAddrs, inTopic),
                applierCfg
            ),
            "kafka-to-ignite-" + inTopic
        );

        return List.of(cdc, applier);
    }

    /** */
    private static Thread runDaemon(Runnable r, String name) {
        Thread t = new Thread(r, name);

        t.setDaemon(true);
        t.start();

        return t;
    }

    /** */
    private static Properties kafkaProperties(String kafkaAddrs, String grpSuffix) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddrs);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier-" + grpSuffix);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        return props;
    }
}
