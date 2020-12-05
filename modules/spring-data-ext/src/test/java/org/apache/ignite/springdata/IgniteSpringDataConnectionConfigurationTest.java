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

package org.apache.ignite.springdata;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.springframework.context.annotation.FilterType.ASSIGNABLE_TYPE;

/** Tests Spring Data repository cluster connection configurations. */
public class IgniteSpringDataConnectionConfigurationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "PersonCache";

    /** */
    private static final int CLI_CONN_PORT = 10810;

    /** Tests repository configuration in case {@link IgniteConfiguration} is used to access the Ignite cluster. */
    @Test
    public void testRepositoryWithIgniteConfiguration() {
        checkRepositoryConfiguration(IgniteConfigurationApplication.class);
    }

    /** Tests repository configuration in case {@link ClientConfiguration} is used to access the Ignite cluster.*/
    @Test
    public void testRepositoryWithClientConfiguration() {
        checkRepositoryConfiguration(ClientConfigurationApplication.class);
    }

    /** Tests repository configuration in case specified cache name is invalid. */
    @Test
    public void testRepositoryWithInvalidCacheNameConfiguration() {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
            ctx.register(InvalidCacheNameApplication.class);

            assertThrowsAnyCause(log,
                () -> {
                    ctx.refresh();

                    return null;
                },
                IllegalArgumentException.class,
                "Invalid repository configuration [name=" + InvalidCacheRepository.class.getName() + "]. Set a" +
                    " name of an Apache Ignite cache using org.apache.ignite.springdata.repository.config.RepositoryConfig" +
                    " annotation to map this repository to the underlying cache.");
        }

        assertTrue(Ignition.allGrids().isEmpty());
    }

    /**
     * Checks that repository created based on specified Spring application configuration is properly initialized and
     * got access to the Ignite cluster.
     */
    private void checkRepositoryConfiguration(Class<?> cfgCls) {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
            ctx.register(cfgCls);
            ctx.refresh();

            PersonRepository repo = ctx.getBean(PersonRepository.class);

            IgniteCache<Integer, Person> cache = ctx.getBean(Ignite.class).cache(CACHE_NAME);

            assertEquals(0, repo.count());
            assertEquals(0, cache.size());

            int key = 0;

            repo.save(key, new Person("Domenico", "Scarlatti"));

            assertEquals(1, repo.count());
            assertNotNull(cache.get(key));
        }

        assertTrue(Ignition.allGrids().isEmpty());
    }

    /**
     * Spring Application configuration for repository testing in case {@link IgniteConfiguration} is used
     * for accessing the cluster.
     */
    @Configuration
    @EnableIgniteRepositories(includeFilters = @Filter(type = ASSIGNABLE_TYPE, classes = PersonRepository.class))
    public static class IgniteConfigurationApplication {
        /** */
        @Bean
        public Ignite igniteServerNode() {
            return Ignition.start(getIgniteConfiguration("srv-node", false));
        }

        /** Ignite configuration bean. */
        @Bean
        public IgniteConfiguration igniteCfg() {
            return getIgniteConfiguration("cli-node", true);
        }
    }

    /**
     * Spring Application configuration for repository testing in case {@link IgniteConfiguration} is used
     * for accessing the cluster.
     */
    @Configuration
    @EnableIgniteRepositories(
        considerNestedRepositories = true,
        includeFilters = @Filter(type = ASSIGNABLE_TYPE, classes = InvalidCacheRepository.class))
    public static class InvalidCacheNameApplication {
        /** */
        @Bean
        public Ignite igniteServerNode() {
            return Ignition.start(getIgniteConfiguration("srv-node", false));
        }

        /** Ignite configuration bean. */
        @Bean
        public IgniteConfiguration igniteCfg() {
            return getIgniteConfiguration("cli-node", true);
        }
    }

    /**
     * Spring Application configuration for repository testing in case {@link ClientConfiguration} is used
     * for accessing the cluster.
     */
    @Configuration
    @EnableIgniteRepositories(includeFilters = @Filter(type = ASSIGNABLE_TYPE, classes = PersonRepository.class))
    public static class ClientConfigurationApplication {
        /** */
        @Bean
        public Ignite igniteServerNode() {
            return Ignition.start(getIgniteConfiguration("srv-node", false));
        }

        /** Ignite client configuration bean. */
        @Bean
        public ClientConfiguration igniteCfg() {
            return new ClientConfiguration().setAddresses("127.0.0.1:" + CLI_CONN_PORT);
        }
    }

    /** Repository for testing application behavior in case the cache name is not specified in the repository configuration. */
    @RepositoryConfig
    interface InvalidCacheRepository extends IgniteRepository<Person, Integer> {
        // No-op.
    }

    /** */
    private static IgniteConfiguration getIgniteConfiguration(String name, boolean clientMode) {
        return new IgniteConfiguration()
            .setIgniteInstanceName(name)
            .setClientMode(clientMode)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setPort(CLI_CONN_PORT))
            .setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME));
    }

}
