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

import java.io.Serializable;
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
import org.apache.ignite.springdata20.repository.IgniteRepository;
import org.apache.ignite.springdata20.repository.config.EnableIgniteRepositories;
import org.apache.ignite.springdata20.repository.config.RepositoryConfig;
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
        checkRepositoryConfiguration(IgniteConfigurationApplication.class, IgniteConfigRepository.class);
    }

    /** Tests repository configuration in case {@link ClientConfiguration} is used to access the Ignite cluster.*/
    @Test
    public void testRepositoryWithClientConfiguration() {
        checkRepositoryConfiguration(ClientConfigurationApplication.class, IgniteClientConfigRepository.class);
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
                "Cache 'invalidCache' not found for repository interface" +
                    " org.apache.ignite.springdata.IgniteSpringDataConnectionConfigurationTest$InvalidCacheNameRepository." +
                    " Please, add a cache configuration to ignite configuration or pass autoCreateCache=true to" +
                    " org.apache.ignite.springdata20.repository.config.RepositoryConfig annotation.");
        }

        assertTrue(Ignition.allGrids().isEmpty());
    }

    /**
     * Checks that repository created based on specified Spring application configuration is properly initialized and
     * got access to the Ignite cluster.
     */
    private void checkRepositoryConfiguration(
        Class<?> cfgCls,
        Class<? extends IgniteRepository<Object, Serializable>> repoCls
    ) {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
            ctx.register(cfgCls);
            ctx.refresh();

            IgniteRepository<Object, Serializable> repo = ctx.getBean(repoCls);

            IgniteCache<Object, Serializable> cache = ctx.getBean(Ignite.class).cache(CACHE_NAME);

            assertEquals(0, repo.count());
            assertEquals(0, cache.size());

            int key = 0;

            repo.save(key, "1");

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
    @EnableIgniteRepositories(
        considerNestedRepositories = true,
        includeFilters = @Filter(type = ASSIGNABLE_TYPE, classes = InvalidCacheNameRepository.class))
    public static class InvalidCacheNameApplication {
        /** */
        @Bean
        public Ignite igniteServerNode() {
            return Ignition.start(getIgniteConfiguration("srv-node", false));
        }

        /** Ignite configuration bean. */
        @Bean
        public IgniteConfiguration igniteConfiguration() {
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
        includeFilters = @Filter(type = ASSIGNABLE_TYPE, classes = IgniteConfigRepository.class))
    public static class IgniteConfigurationApplication {
        /** */
        @Bean
        public Ignite igniteServerNode() {
            return Ignition.start(getIgniteConfiguration("srv-node", false));
        }

        /** Ignite configuration bean. */
        @Bean
        public IgniteConfiguration igniteConfiguration() {
            return getIgniteConfiguration("cli-node", true);
        }
    }

    /**
     * Spring Application configuration for repository testing in case {@link ClientConfiguration} is used
     * for accessing the cluster.
     */
    @Configuration
    @EnableIgniteRepositories(
        considerNestedRepositories = true,
        includeFilters = @Filter(type = ASSIGNABLE_TYPE, classes = IgniteClientConfigRepository.class))
    public static class ClientConfigurationApplication {
        /** */
        @Bean
        public Ignite igniteServerNode() {
            return Ignition.start(getIgniteConfiguration("srv-node", false));
        }

        /** Ignite client configuration bean. */
        @Bean
        public ClientConfiguration clientConfiguration() {
            return new ClientConfiguration().setAddresses("127.0.0.1:" + CLI_CONN_PORT);
        }
    }

    /** Repository for testing configuration approach through {@link IgniteConfiguration}. */
    @RepositoryConfig(cacheName = "PersonCache", igniteCfg = "igniteConfiguration")
    public interface IgniteConfigRepository extends IgniteRepository<Object, Serializable> {
        // No-op.
    }

    /** Repository for testing repository configuration approach through {@link ClientConfiguration}. */
    @RepositoryConfig(cacheName = "PersonCache", igniteCfg = "clientConfiguration")
    public interface IgniteClientConfigRepository extends IgniteRepository<Object, Serializable> {
        // No-op.
    }

    /** Repository for testing configuration in case invalid cache name is specified. */
    @RepositoryConfig(cacheName = "invalidCache", igniteCfg = "igniteConfiguration")
    public interface InvalidCacheNameRepository extends IgniteRepository<Object, Serializable> {
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
