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

package org.apache.ignite.stream.kafka;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.apache.curator.test.TestingServer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;

/**
 * Kafka Test Broker.
 */
public class TestKafkaBroker {
    /** ZooKeeper connection timeout. */
    private static final int ZK_CONNECTION_TIMEOUT = 6000;

    /** ZooKeeper session timeout. */
    private static final int ZK_SESSION_TIMEOUT = 6000;

    /** ZooKeeper port. */
    private static final int ZK_PORT = 21811;

    /** Broker host. */
    private static final String BROKER_HOST = "localhost";

    /** Broker port. */
    private static final int BROKER_PORT = 11092;

    /** Kafka config. */
    private final KafkaConfig kafkaCfg;

    /** Kafka server. */
    private final KafkaServer kafkaSrv;

    /** ZooKeeper. */
    private final TestingServer zkServer;

    /** Kafka Admin Client. */
    private AdminClient admin;

    /**
     * Kafka broker constructor.
     */
    public TestKafkaBroker() {
        try {
            zkServer = new TestingServer(ZK_PORT, true);
            kafkaCfg = new KafkaConfig(getKafkaConfig());
            kafkaSrv = TestUtils.createServer(kafkaCfg, new SystemTime());

            kafkaSrv.startup();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start Kafka: " + e);
        }
    }

    /**
     * @return Existing admin client or creates new
     * @throws IOException
     */
    public synchronized AdminClient admin() throws IOException {
        if (admin == null)
            admin = AdminClient.create(getKafkaConfig());

        return admin;
    }

    /**
     * Creates a topic.
     *
     * @param topic Topic name.
     * @param partitions Number of partitions for the topic.
     * @param replicationFactor Replication factor.
     * @throws TimeoutException If operation is timed out.
     * @throws InterruptedException If interrupted.
     */
    public void createTopic(String topic, int partitions, int replicationFactor)
        throws IOException, ExecutionException, InterruptedException {
        admin().createTopics(Collections.singleton(new NewTopic(topic, partitions, (short)replicationFactor)))
            .all().get();
    }

    /**
     * Sends a message to Kafka broker.
     *
     * @param records List of records.
     */
    public void sendMessages(List<ProducerRecord<String, String>> records) {
        try (Producer<String, String> producer = new KafkaProducer<>(getProducerConfig())) {
            for (ProducerRecord<String, String> rec : records)
                producer.send(rec);
        }
    }

    /**
     * Shuts down test Kafka broker.
     */
    public void shutdown() {
        if (admin != null)
            admin.close();

        assert kafkaSrv != null;
        assert zkServer != null;

        kafkaSrv.shutdown();

        try {
            zkServer.stop();
        }
        catch (IOException ignored) {
            // No-op.
        }

        List<String> logDirs = scala.collection.JavaConversions.seqAsJavaList(kafkaCfg.logDirs());

        for (String logDir : logDirs)
            U.delete(new File(logDir));
    }

    /**
     * Obtains Kafka config.
     *
     * @return Kafka config.
     * @throws IOException If failed.
     */
    private Properties getKafkaConfig() throws IOException {
        Properties props = new Properties();

        props.put("broker.id", "0");
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("host.name", BROKER_HOST);
        props.put("port", BROKER_PORT);
        props.put("bootstrap.servers", getBrokerAddress());
        props.put("offsets.topic.replication.factor", "1");
        props.put("log.dir", createTmpDir("_cfg").getAbsolutePath());
        props.put("log.flush.interval.messages", "1");
        props.put("log.flush.interval.ms", "10");
        props.put("listeners", "PLAINTEXT://" + getBrokerAddress());

        return props;
    }

    /**
     * Obtains broker address.
     *
     * @return Kafka broker address.
     */
    public String getBrokerAddress() {
        return BROKER_HOST + ":" + BROKER_PORT;
    }

    /**
     * Obtains producer config.
     *
     * @return Kafka Producer config.
     */
    private Properties getProducerConfig() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerAddress());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaTestProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    /**
     * Creates temporary directory.
     *
     * @param prefix Prefix.
     * @return Created file.
     * @throws IOException If failed.
     */
    private static File createTmpDir(String prefix) throws IOException {
        Path path = Files.createTempDirectory(prefix);

        return path.toFile();
    }
}
