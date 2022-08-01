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

package org.apache.ignite.cdc.thin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.cdc.AbstractReplicationTest;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.IgniteToIgniteCdcStreamer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * {@link IgniteToIgniteClientCdcStreamer} test.
 */
public class CdcIgniteToIgniteClientReplicationTest extends AbstractReplicationTest {
    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActivePassiveCdc(String cache) {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToIgniteClient(srcCluster[i].configuration(), destCluster, cache));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActiveActiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToIgniteClient(srcCluster[i].configuration(), destCluster, ACTIVE_ACTIVE_CACHE));

        for (int i = 0; i < destCluster.length; i++)
            futs.add(igniteToIgniteClient(destCluster[i].configuration(), srcCluster, ACTIVE_ACTIVE_CACHE));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected void checkConsumerMetrics(Function<String, Long> longMetric) {
        assertNotNull(longMetric.apply(IgniteToIgniteCdcStreamer.LAST_EVT_TIME));
        assertNotNull(longMetric.apply(IgniteToIgniteCdcStreamer.EVTS_CNT));
    }

    /**
     * @param srcCfg Ignite source node configuration.
     * @param destCfg Ignite destination cluster configuration.
     * @param cache Cache name to replicate.
     * @return Future for Change Data Capture application.
     */
    private IgniteInternalFuture<?> igniteToIgniteClient(IgniteConfiguration srcCfg, IgniteEx[] dest, String cache) {
        return runAsync(() -> {
            ClientConfiguration clientCfg = new ClientConfiguration();

            String[] addrs = new String[dest.length];

            for (int i = 0; i < dest.length; i++) {
                ClusterNode node = dest[i].localNode();

                addrs[i] = F.first(node.addresses()) + ":" + node.attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT);
            }

            clientCfg.setAddresses(addrs);

            CdcConfiguration cdcCfg = new CdcConfiguration();

            cdcCfg.setConsumer(new IgniteToIgniteClientCdcStreamer()
                .setDestinationClientConfiguration(clientCfg)
                .setCaches(Collections.singleton(cache))
                .setMaxBatchSize(KEYS_CNT));

            cdcCfg.setMetricExporterSpi(new JmxMetricExporterSpi());

            CdcMain cdc = new CdcMain(srcCfg, null, cdcCfg);

            cdcs.add(cdc);

            cdc.run();
        });
    }

    /** {@inheritDoc} */
    @Override protected boolean metadataReplicationSupported() {
        return false;
    }
}
