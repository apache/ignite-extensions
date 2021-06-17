package org.apache.ignite.spring.sessions;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

/**
 * Utility class for Ignite integration tests.
 */
final class IgniteITestUtils {

    private IgniteITestUtils() {
    }

    /**
     * Creates {@link Ignite} for use in integration tests.
     * @return the Ignite instance
     */
    static Ignite embeddedIgniteServer() {
        return Ignition.start();
    }

}
