package org.apache.ignite.testsuites;

import org.apache.ignite.spring.sessions.EmbeddedIgniteIndexedSessionRepositoryITest;
import org.apache.ignite.spring.sessions.IgniteHttpSessionConfigurationTest;
import org.apache.ignite.spring.sessions.IgniteIndexedSessionRepositoryTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Ignite Spring Session Test Suite
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        EmbeddedIgniteIndexedSessionRepositoryITest.class,
        IgniteHttpSessionConfigurationTest.class,
        IgniteIndexedSessionRepositoryTest.class
})
public class IgniteSpringSessionTestSuite {
}
