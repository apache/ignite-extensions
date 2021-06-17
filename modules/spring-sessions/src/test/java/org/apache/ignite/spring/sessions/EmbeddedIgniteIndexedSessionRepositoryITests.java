package org.apache.ignite.spring.sessions;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;

/**
 * Integration tests for {@link IgniteIndexedSessionRepository} using embedded topology.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@WebAppConfiguration
class EmbeddedIgniteIndexedSessionRepositoryITests extends AbstractIgniteIndexedSessionRepositoryITests {

    @BeforeAll
    static void setUpClass() {
        Ignition.stopAll(true);
    }

    @AfterAll
    static void tearDownClass() {
        Ignition.stopAll(true);
    }

    @EnableIgniteHttpSession
    @Configuration
    static class IgniteSessionConfig {

        @Bean
        Ignite ignite() {
            return IgniteITestUtils.embeddedIgniteServer();
        }

    }

}