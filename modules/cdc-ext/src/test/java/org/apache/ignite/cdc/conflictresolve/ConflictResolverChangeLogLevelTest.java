package org.apache.ignite.cdc.conflictresolve;

import java.util.Collections;
import java.util.HashSet;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;

import static org.apache.logging.log4j.Level.INFO;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;
import static org.apache.logging.log4j.core.config.Configurator.setRootLevel;

/** Check switching conflict resolver log level during runtime. */
public class ConflictResolverChangeLogLevelTest extends GridCommonAbstractTest {
    /** */
    MetricRegistryImpl mreg = new MetricRegistryImpl("group", name -> null, name -> null, null);

    /** Listener test logger. */
    private ListeningTestLogger lsnrLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheVersionConflictResolverPluginProvider<?> pluginCfg = new CacheVersionConflictResolverPluginProvider<>();

        pluginCfg.setClusterId((byte)1);
        pluginCfg.setConflictResolver(new CacheVersionConflictResolverImpl((byte)1, null, log, mreg));
        pluginCfg.setCaches(new HashSet<>(Collections.singleton(DEFAULT_CACHE_NAME)));

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)))
            .setPluginProviders(pluginCfg)
            .setGridLogger(lsnrLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        Configurator.setRootLevel(Level.INFO);

        Configurator.setLevel(CacheVersionConflictResolverImpl.class.getName(), Level.INFO);

        assertEquals(INFO, LoggerContext.getContext(false).getConfiguration().getRootLogger().getLevel());

        lsnrLog.clearListeners();

        stopAllGrids();

        super.afterTestsStopped();
    }

    /** Tests {@code clusterId} value set correctly after node restart. */
    @Test
    public void testSwitchingLogLevelForConflictResolver() throws Exception {
        LogListener lsnr = LogListener.matches("isUseNew").build();

        lsnrLog.registerListener(lsnr);

        IgniteEx ign = startGrid(1);

        ign.cluster().state(ClusterState.ACTIVE);

        ign.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        assertFalse(lsnr.check());

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("isUseNew").build();

        lsnrLog.registerListener(lsnr);

        Configurator.setRootLevel(Level.DEBUG);

        Configurator.setLevel(CacheVersionConflictResolverImpl.class.getName(), Level.DEBUG);

        ign.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        //assertTrue(lsnr.check());

        assertTrue(CacheConflictResolutionManagerImpl.conflictResolverLog.isDebugEnabled());
    }
}