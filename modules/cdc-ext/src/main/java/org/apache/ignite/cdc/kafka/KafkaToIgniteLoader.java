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

import java.net.URL;
import java.util.Collection;
import java.util.Properties;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Utility class to load {@link KafkaToIgniteCdcStreamer} from Spring XML configuration.
 */
public class KafkaToIgniteLoader {
    /** Kafka properties bean name. */
    public static final String KAFKA_PROPERTIES = "kafkaProperties";

    /**
     * Loads {@link KafkaToIgniteCdcStreamer} from XML configuration file.
     * If load fails then error message wouldn't be null.
     *
     * @param springXmlPath Path to XML configuration file.
     * @return {@code KafkaToIgniteCdcStreamer} instance.
     * @throws IgniteCheckedException If failed.
     */
    public static KafkaToIgniteCdcStreamer loadKafkaToIgniteStreamer(String springXmlPath) throws IgniteCheckedException {
        URL cfgUrl = U.resolveSpringUrl(springXmlPath);

        IgniteSpringHelper spring = SPRING.create(false);

        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgTuple =
            spring.loadConfigurations(cfgUrl);

        if (cfgTuple.get1().size() > 1) {
            throw new IgniteCheckedException(
                "Exact 1 IgniteConfiguration should be defined. Found " + cfgTuple.get1().size()
            );
        }

        IgniteBiTuple<Collection<KafkaToIgniteCdcStreamerConfiguration>, ? extends GridSpringResourceContext> k2iCfg =
            spring.loadConfigurations(cfgUrl, KafkaToIgniteCdcStreamerConfiguration.class);

        if (k2iCfg.get1().size() > 1) {
            throw new IgniteCheckedException(
                "Exact 1 KafkaToIgniteCdcStreamerConfiguration configuration should be defined. " +
                    "Found " + k2iCfg.get1().size()
            );
        }

        Properties kafkaProps = spring.loadBean(cfgUrl, KAFKA_PROPERTIES);

        return new KafkaToIgniteCdcStreamer(cfgTuple.get1().iterator().next(), kafkaProps, k2iCfg.get1().iterator().next());
    }
}
