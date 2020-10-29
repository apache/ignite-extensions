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

package org.apache.ignite.springdata.misc;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata22.repository.config.EnableIgniteRepositories;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.springframework.context.annotation.ComponentScan.Filter;
import static org.springframework.context.annotation.FilterType.ASSIGNABLE_TYPE;

/**
 * Spring Application configuration for testing failure of repository initialization in case thin client is used for
 * accessing the cluster and text queries are configured for any repository methods.
 */
@Configuration
@EnableIgniteRepositories(includeFilters = @Filter(type = ASSIGNABLE_TYPE, value = IgnitePersonRepository.class))
public class IgniteClientUnsupportedApplicationConfiguration {
    /** Ignite client instance. */
    @Bean
    public IgniteClient igniteInstance() {
        Ignition.start(new IgniteConfiguration().setClientConnectorConfiguration(new ClientConnectorConfiguration()));

        return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT));
    }
}
