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

package org.apache.ignite.cdc.metrics;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;

import static org.apache.ignite.internal.IgnitionEx.initializeDefaultMBeanServer;

/** */
public final class MetricsUtils {
    /** Default constructor. */
    private MetricsUtils() {

    }

    /**
     * @param log {@link IgniteLogger}
     * @param streamerCfg {@link KafkaToIgniteCdcStreamerConfiguration}
     * @return {@link StandaloneGridKernalContext} for {@link MetricRegistryImpl} initialization
     */
    public static StandaloneGridKernalContext createStandaloneGridKernalContext(
        IgniteLogger log,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg) throws IgniteCheckedException {
        return new StandaloneGridKernalContext(log, null, null) {
            @Override protected IgniteConfiguration prepareIgniteConfiguration() {
                IgniteConfiguration cfg = super.prepareIgniteConfiguration();

                cfg.setIgniteInstanceName(streamerCfg.getMetricRegistryName());

                if (!F.isEmpty(streamerCfg.getMetricExporterSpi()))
                    cfg.setMetricExporterSpi(streamerCfg.getMetricExporterSpi());
                else {
                    cfg.setMetricExporterSpi(U.IGNITE_MBEANS_DISABLED
                        ? new NoopMetricExporterSpi()
                        : new JmxMetricExporterSpi());
                }

                initializeDefaultMBeanServer(cfg);

                return cfg;
            }

            /** {@inheritDoc} */
            @Override public String igniteInstanceName() {
                return config().getIgniteInstanceName();
            }
        };
    }
}
