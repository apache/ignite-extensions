package org.apache.ignite.plugin.mdc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class MdcSegmentationBasicTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginProviders(new SegmentationProtectionPluginProvider());

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        return super.optimize(cfg).setIncludeProperties((String[])null);
    }

    /** */
    @Test
    public void testCacheOperationsAreBlockedOnSecondary() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, "dc0");

        // dc0 is considered main as it is started first.
        startGrid(0);

        // Calculation of topology validity doesn't consider client nodes.
        startClientGrid("clientDc0");

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, "dc1");

        IgniteEx dc1Ig = startGrid(1);

        dc1Ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = createCache(dc1Ig);

        try {
            cache.put(1, 1);
        }
        catch (Exception e) {
            assertTrue("Unexpected exception was thrown: " + e, false);
        }

        stopGrid(0);

        try {
            cache.put(1, 2);

            assertTrue("Expected exception was not thrown", false);
        }
        catch (Exception e) {
            // No-op.
        }
    }

    /** */
    @Test
    public void testPromoteSecondaryToMain() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, "dc0");

        // dc0 is considered main as it is starts first
        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, "dc1");

        IgniteEx dc1Ig = startGrid(1);

        dc1Ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = createCache(dc1Ig);

        stopGrid(0);

        try {
            cache.put(1, 1);

            assertTrue("Expected exception was not thrown", false);
        }
        catch (Exception e) {
            // No-op.
        }

        assertTrue(dc1Ig.context().distributedMetastorage().compareAndSet("mainDc", "dc0", "dc1"));

        startClientGrid("clientGridDc1");

        try {
            cache.put(1, 2);
        }
        catch (Exception e) {
            assertTrue("Unexpected exception was thrown: " + e, false);
        }
    }

    /** */
    @Test
    public void testCacheOperationsAreNotBlockedOnNodesFromMainDc() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, "dc0");

        // dc0 is considered main as it is starts first
        IgniteEx dc0Ig = startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, "dc1");

        startGrid(1);

        dc0Ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = createCache(dc0Ig);

        try {
            cache.put(1, 1);
        }
        catch (Exception e) {
            assertTrue("Unexpected exception was thrown: " + e, false);
        }

        stopGrid(1);

        try {
            cache.put(1, 2);
        }
        catch (Exception e) {
            // No-op.
        }
    }

    /** */
    public IgniteCache<Object, Object> createCache(IgniteEx grid) {
        return grid.createCache(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC));
    }
}
