package org.apache.ignite.spring.sessions;

import java.util.Collections;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;

/**
 * Integration tests for {@link IgniteIndexedSessionRepository} using client-server
 * topology.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@WebAppConfiguration
class ClientServerIgniteIndexedSessionRepositoryITests extends AbstractIgniteIndexedSessionRepositoryITests {

    private static GenericContainer container = new GenericContainer<>("apacheignite/ignite:2.9.0")
            .withExposedPorts(47100, 47500);

    @BeforeAll
    static void setUpClass() {
        Ignition.stopAll(true);
        container.start();
    }

    @AfterAll
    static void tearDownClass() {
        Ignition.stopAll(true);
        container.stop();
    }

    @Configuration
    @EnableIgniteHttpSession
    static class IgniteSessionConfig {

        @Bean
        Ignite ignite() {
            IgniteConfiguration cfg = new IgniteConfiguration();
            final String address = container.getContainerIpAddress() + ":" + container.getMappedPort(47500);
            final TcpDiscoverySpi spi = new TcpDiscoverySpi()
                    .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Collections.singleton(address)));
            cfg.setDiscoverySpi(spi);
            return Ignition.start(cfg);
        }

    }

}
