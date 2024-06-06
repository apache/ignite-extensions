package org.apache.ignite.cdc.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.Test;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests the kafka topic partitions distribution over specified number of threads. */
public class KafkaToIgnitePartitionDistributionTest extends GridCommonAbstractTest {
    /** Active nodes at destination. */
    protected static IgniteEx[] destCluster;

    /** Configurations for client nodes. */
    protected static IgniteConfiguration[] destClusterCliCfg;

    /** Source topic name for data. */
    public static final String DEST_SRC_TOPIC = "dest-source";

    /** Source topic name for metadata. */
    public static final String SRC_DEST_META_TOPIC = "source-dest-meta";

    /** Partitions number. */
    public static final int DFLT_PARTS = 48;

    /** Thread counts. */
    public static final int[] DFLT_THREAD_CNT = new int[] {7, 8, 9};

    /** Kafka. */
    private static EmbeddedKafkaCluster KAFKA = null;

    /** Listening logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** Kafka topic partitions distribution listener. */
    private static final LogListener KAFKA_TOPIC_LOG_1 = LogListener.matches("Kafka topic partitions distribution " +
            "[totalKafkaParts=16, from=0, to=16, threadCnt=7, distribution=[applier-thread-0 [from=0, to=3], " +
            "applier-thread-1 [from=3, to=6], applier-thread-2 [from=6, to=8], applier-thread-3 [from=8, to=10], " +
            "applier-thread-4 [from=10, to=12], applier-thread-5 [from=12, to=14], applier-thread-6 [from=14, to=16]]]")
        .times(1)
        .build();

    /** Kafka topic partitions distribution listener. */
    private static final LogListener KAFKA_TOPIC_LOG_2 = LogListener.matches("Kafka topic partitions distribution " +
            "[totalKafkaParts=16, from=16, to=32, threadCnt=8, distribution=[applier-thread-0 [from=16, to=18], " +
            "applier-thread-1 [from=18, to=20], applier-thread-2 [from=20, to=22], applier-thread-3 [from=22, to=24], " +
            "applier-thread-4 [from=24, to=26], applier-thread-5 [from=26, to=28], applier-thread-6 [from=28, to=30], " +
            "applier-thread-7 [from=30, to=32]]]")
        .times(1)
        .build();

    /** Kafka topic partitions distribution listener. */
    private static final LogListener KAFKA_TOPIC_LOG_3 = LogListener.matches("Kafka topic partitions distribution " +
            "[totalKafkaParts=16, from=32, to=48, threadCnt=9, distribution=[applier-thread-0 [from=32, to=34], " +
            "applier-thread-1 [from=34, to=36], applier-thread-2 [from=36, to=38], applier-thread-3 [from=38, to=40], " +
            "applier-thread-4 [from=40, to=42], applier-thread-5 [from=42, to=44], applier-thread-6 [from=44, to=46], " +
            "applier-thread-7 [from=46, to=47], applier-thread-8 [from=47, to=48]]]")
        .times(1)
        .build();

    /** Kafka logs. */
    private static final List<LogListener> KAFKA_LOGS = Arrays.asList(KAFKA_TOPIC_LOG_1, KAFKA_TOPIC_LOG_2, KAFKA_TOPIC_LOG_3);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteBiTuple<IgniteEx[], IgniteConfiguration[]> cluster = setupCluster();

        destCluster = cluster.get1();
        destClusterCliCfg = cluster.get2();

        KAFKA = initKafka(KAFKA);

        for(LogListener logLsnr : KAFKA_LOGS)
            listeningLog.registerListener(logLsnr);
    }

    /**
     * Destination cluster setup. Starts cluster with 3 nodes and configures client configuration for each of them.
     * @return {@link IgniteBiTuple} with {@link IgniteEx} as first element and {@link IgniteConfiguration} as last.
     */
    private IgniteBiTuple<IgniteEx[], IgniteConfiguration[]> setupCluster() throws Exception {
        IgniteEx[] cluster = new IgniteEx[] {
            startGrid(0),
            startGrid(1),
            startGrid(2)
        };

        IgniteConfiguration[] clusterCliCfg = new IgniteConfiguration[cluster.length];

        String clientPrefix = "dest-cluster-client";

        for (int i = 0; i < cluster.length; i++) {
            IgniteConfiguration cliCfg = getConfiguration(clientPrefix + i)
                .setClientMode(true)
                .setGridLogger(listeningLog);

            clusterCliCfg[i] = optimize(cliCfg);
        }

        cluster[0].cluster().tag("destination");

        return F.t(cluster, clusterCliCfg);
    }

    /**
     * Initialises Kafka and starts 2 topics for {@link KafkaToIgniteCdcStreamer}.
     * @param curKafka Current kafka.
     */
    private static EmbeddedKafkaCluster initKafka(EmbeddedKafkaCluster curKafka) throws Exception {
        EmbeddedKafkaCluster kafka = curKafka;

        if (kafka == null) {
            Properties props = new Properties();

            props.put("auto.create.topics.enable", "false");

            kafka = new EmbeddedKafkaCluster(1, props);

            kafka.start();
        }

        kafka.createTopic(DEST_SRC_TOPIC, DFLT_PARTS, 1);
        kafka.createTopic(SRC_DEST_META_TOPIC, 1, 1);

        return kafka;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        removeKafkaTopicsAndWait(KAFKA, getTestTimeout());
    }

    /**
     * Deletes kafka topics and waits for completion.
     * @param kafka Kafka cluster.
     * @param timeout Timeout.
     */
    static void removeKafkaTopicsAndWait(EmbeddedKafkaCluster kafka, long timeout) throws IgniteInterruptedCheckedException {
        kafka.getAllTopicsInCluster().forEach(t -> {
            try {
                kafka.deleteTopic(t);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        waitForCondition(() -> kafka.getAllTopicsInCluster().isEmpty(), timeout);
    }

    /**
     * Checks the topic partitions distribution over specified number of threads.
     */
    @Test
    public void testActivePassiveReplication() throws Exception {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < destCluster.length; i++) {
            futs.add(kafkaToIgnite(
                destClusterCliCfg[i],
                i * (DFLT_PARTS / 3),
                (i + 1) * (DFLT_PARTS / 3),
                DFLT_THREAD_CNT[i]
            ));
        }

        for(LogListener logLsnr : KAFKA_LOGS)
            assertTrue(logLsnr.check(getTestTimeout()));

        for (IgniteInternalFuture<?> fut : futs)
            fut.cancel();
    }

    /**
     * Kafka to Ignite streamer configuration. Asynchronously starts {@link KafkaToIgniteCdcStreamer}.
     * @param igniteCfg {@link IgniteConfiguration} for client node.
     * @param fromPart starting index for kafka topic partition.
     * @param toPart ending index for kafka topic partition.
     * @param threadCnt number of threads, that will process specified partitions.
     * @return Future for run {@link KafkaToIgniteCdcStreamer}.
     */
    protected IgniteInternalFuture<?> kafkaToIgnite(
        IgniteConfiguration igniteCfg,
        int fromPart,
        int toPart,
        int threadCnt
    ) {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();

        cfg.setKafkaPartsFrom(fromPart);
        cfg.setKafkaPartsTo(toPart);
        cfg.setThreadCount(threadCnt);

        cfg.setTopic(DEST_SRC_TOPIC);
        cfg.setMetadataTopic(SRC_DEST_META_TOPIC);
        cfg.setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

        return runAsync(new KafkaToIgniteCdcStreamer(igniteCfg, kafkaProperties(), cfg));
    }

    /** */
    private Properties kafkaProperties() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        return props;
    }
}
