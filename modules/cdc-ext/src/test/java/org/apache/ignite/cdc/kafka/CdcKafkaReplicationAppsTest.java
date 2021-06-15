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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.startup.cmdline.ChangeDataCaptureCommandLineStartup;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_PARTS;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class CdcKafkaReplicationAppsTest extends CdcKafkaReplicationTest {
    /** */
    public static final String INSTANCE_NAME = "INSTANCE_NAME";

    /** */
    public static final String COMM_PORT = "COMM_PORT";

    /** */
    public static final String DISCO_PORT = "DISCO_PORT";

    /** */
    public static final String DISCO_PORT_RANGE = "DISCO_PORT_RANGE";

    /** */
    public static final String REPLICATED_CACHE = "REPLICATED_CACHE";

    /** */
    public static final String TOPIC = "TOPIC";

    /** */
    public static final String CONSISTENT_ID = "CONSISTENT_ID";

    /** */
    public static final String KAFKA_PARTS = "KAFKA_PARTS";

    /** */
    public static final String MAX_BATCH_SIZE = "MAX_BATCH_SIZE";

    /** */
    public static final String KAFKA_PROPS_PATH = "KAFKA_PROPS_PATH";

    /** */
    private String kafkaPropsPath = null;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (kafkaPropsPath == null) {
            File file = File.createTempFile("kafka", "properties");

            file.deleteOnExit();

            try (FileOutputStream fos = new FileOutputStream(file)) {
                props.store(fos, null);
            }

            kafkaPropsPath = "file://" + file.getAbsolutePath();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<?> igniteToKafka(IgniteConfiguration igniteCfg, String topic, String cache) {
        Map<String, String> params = new HashMap<>();

        params.put(INSTANCE_NAME, igniteCfg.getIgniteInstanceName());
        params.put(REPLICATED_CACHE, cache);
        params.put(TOPIC, topic);
        params.put(CONSISTENT_ID, String.valueOf(igniteCfg.getConsistentId()));
        params.put(KAFKA_PARTS, Integer.toString(DFLT_PARTS));
        params.put(MAX_BATCH_SIZE, Integer.toString(KEYS_CNT));
        params.put(KAFKA_PROPS_PATH, kafkaPropsPath);

        return runAsync(
            () -> ChangeDataCaptureCommandLineStartup.main(new String[] {prepareConfig("replication/ignite-to-kafka.xml", params)})
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<?> kafkaToIgnite(
        String cacheName,
        String topic,
        IgniteConfiguration igniteCfg
    ) {
        Map<String, String> params = new HashMap<>();

        int discoPort = getFieldValue(igniteCfg.getDiscoverySpi(), "locPort");

        params.put(INSTANCE_NAME, igniteCfg.getIgniteInstanceName());
        params.put(COMM_PORT, Integer.toString(((TcpCommunicationSpi)igniteCfg.getCommunicationSpi()).getLocalPort()));
        params.put(DISCO_PORT, Integer.toString(discoPort));
        params.put(DISCO_PORT_RANGE, Integer.toString(discoPort + DFLT_PORT_RANGE));
        params.put(REPLICATED_CACHE, cacheName);
        params.put(TOPIC, topic);
        params.put(KAFKA_PROPS_PATH, kafkaPropsPath);

        return runAsync(
            () -> KafkaToIgniteCommandLineStartup.main(new String[] {prepareConfig("replication/kafka-to-ignite.xml", params)})
        );
    }

    /** */
    private String prepareConfig(String path, Map<String, String> params) {
        try {
            String cfg = new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource(path).toURI())));

            for (String key : params.keySet()) {
                String subst = '{' + key + '}';

                while (cfg.contains(subst))
                    cfg = cfg.replace(subst, params.get(key));
            }

            File file = File.createTempFile("ignite-config", "xml");

            file.deleteOnExit();

            try (PrintWriter out = new PrintWriter(file)) {
                out.print(cfg);
            }

            return file.getAbsolutePath();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}