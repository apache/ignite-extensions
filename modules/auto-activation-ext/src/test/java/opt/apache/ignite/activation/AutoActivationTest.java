package opt.apache.ignite.activation;

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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * {@link AutoActivationPluginProvider} test
 */
public class AutoActivationTest extends GridCommonAbstractTest {
    /** Listening test logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    private final LogListener lsnrAlreadyAct = LogListener
            .matches("Auto activation skipped - cluster already activated").build();

    /** */
    private final LogListener lsnrBaseline = LogListener
            .matches("Auto activation skipped - baseline is not empty").build();

    /** */
    private final LogListener lsnrActMeet = LogListener
            .matches("Auto activation plugin set cluster state ACTIVE - activation condition meet").build();

    /** */
    private final LogListener lsnrActNotMeet = LogListener
            .matches("Auto activation skipped - activation condition not meet").build();

    /** */
    private final String NODE_0 = "node_0";

    /** */
    private final String NODE_1 = "node_1";

    /** */
    private final String NODE_2 = "node_2";

    /** */
    private final String NODE_3 = "node_3";

    /** */
    private final String ATTR = "CELL";

    /** */
    private final String ATTR_VAL1 = "CELL_01";

    /** */
    private final String ATTR_VAL2 = "CELL_02";

    /** */
    private final Set<String> nodesConsistentIds = Set.of(NODE_0, NODE_1, NODE_2);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        listeningLog.registerAllListeners(lsnrAlreadyAct, lsnrBaseline, lsnrActMeet, lsnrActNotMeet);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();

        listeningLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration igniteConfiguration = super.getConfiguration(igniteInstanceName);

        switch (igniteInstanceName) {
            case NODE_0:
                igniteConfiguration.setConsistentId(NODE_0);
                break;

            case NODE_1:
                igniteConfiguration.setConsistentId(NODE_1);
                break;

            case NODE_2:
                igniteConfiguration.setConsistentId(NODE_2);
                break;

            default: throw new IllegalArgumentException("Unknown node: " + igniteInstanceName);
        }

        igniteConfiguration.setClusterStateOnStart(ClusterState.INACTIVE);
        igniteConfiguration.setGridLogger(listeningLog);

        return igniteConfiguration;
    }

    /** @return DataStorageConfiguration. */
    private DataStorageConfiguration getDataStorageConfiguration() {
        return new DataStorageConfiguration()
                .setWalSegmentSize(4 * 1024 * 1024)
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointFrequency(1000)
                .setWalCompactionEnabled(true)
                .setDefaultDataRegionConfiguration(getDataRegionConfiguration());
    }

    /** @return DataRegionConfiguration. */
    private DataRegionConfiguration getDataRegionConfiguration() {
        return new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(100L * 1024 * 1024);
    }

    /** @return CacheConfiguration. */
    private CacheConfiguration getCacheConfiguration() {
        return new CacheConfiguration<>()
                .setName(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(0)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setIndexedTypes(String.class, Integer.class);
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdAllNodes() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByConsistentID(nodesConsistentIds)
        );

        try (IgniteEx node0 = startGrid(getConfiguration(NODE_0).setPluginProviders(autoActivationProvider))) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_1).setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_2).setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdFirstTwoNodes() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByConsistentID(Set.of(NODE_0, NODE_1))
        );

        try (IgniteEx node0 = startGrid(getConfiguration(NODE_0).setPluginProviders(autoActivationProvider))) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_1).setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActMeet.check());
            assertFalse(lsnrAlreadyAct.check());
            assertEquals(node0.cluster().state(), ACTIVE);

            startGrid(getConfiguration(NODE_2).setPluginProviders(autoActivationProvider));

            assertTrue(lsnrAlreadyAct.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdOnlyLastNode() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByConsistentID(Set.of(NODE_2))
        );

        try (IgniteEx node0 = startGrid(getConfiguration(NODE_0).setPluginProviders(autoActivationProvider))) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_1).setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_2).setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdAllNodesPlusCacheConfig() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByConsistentID(Set.of(NODE_2))
        );

        try (IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                .setPluginProviders(autoActivationProvider)
                .setCacheConfiguration(getCacheConfiguration()))) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_1)
                    .setPluginProviders(autoActivationProvider)
                    .setCacheConfiguration(getCacheConfiguration()));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_2)
                    .setPluginProviders(autoActivationProvider)
                    .setCacheConfiguration(getCacheConfiguration()));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testActivationNotMetInMemoryClusterActivationByConsistentId() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByConsistentID(Set.of(NODE_3))
        );

        try (IgniteEx node0 = startGrid(getConfiguration(NODE_0).setPluginProviders(autoActivationProvider))) {
            startGrid(getConfiguration(NODE_1).setPluginProviders(autoActivationProvider));
            startGrid(getConfiguration(NODE_2).setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);
        }
    }

    /** */
    @Test
    public void testAlreadyActivatedInMemoryClusterActivationByConsistentId() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByConsistentID(Set.of(NODE_0))
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                .setClusterStateOnStart(ACTIVE)
                .setPluginProviders(autoActivationProvider))
        ) {
            assertTrue(lsnrAlreadyAct.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testBaselineNotEmptyPersistenceClusterActivationByConsistentId() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByConsistentID(nodesConsistentIds)
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                .setDataStorageConfiguration(getDataStorageConfiguration())
                .setPluginProviders(autoActivationProvider))
        ) {
            startGrid(getConfiguration(NODE_1)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setPluginProviders(autoActivationProvider));

            startGrid(getConfiguration(NODE_2)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);

            node0.cluster().state(INACTIVE);

            stopAllGrids();

            IgniteEx restartedNode0 = startGrid(getConfiguration(NODE_0)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setPluginProviders(autoActivationProvider));

            startGrid(getConfiguration(NODE_1)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setPluginProviders(autoActivationProvider));

            startGrid(getConfiguration(NODE_2)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrBaseline.check());
            assertEquals(restartedNode0.cluster().state(), INACTIVE);
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeAllAttrs() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByNodeAttribute(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2))
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                        .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                        .setPluginProviders(autoActivationProvider))
        ) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_1)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_2)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL2))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeFirstAttr() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByNodeAttribute(ATTR, Set.of(ATTR_VAL1))
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                        .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                        .setPluginProviders(autoActivationProvider))
        ) {
            assertTrue(lsnrActMeet.check());
            assertFalse(lsnrAlreadyAct.check());
            assertEquals(node0.cluster().state(), ACTIVE);

            startGrid(getConfiguration(NODE_1)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrAlreadyAct.check());
            assertEquals(node0.cluster().state(), ACTIVE);

            startGrid(getConfiguration(NODE_2)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL2))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrAlreadyAct.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeLastAttr() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByNodeAttribute(ATTR, Set.of(ATTR_VAL2))
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                        .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                        .setPluginProviders(autoActivationProvider))
        ) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_1)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_2)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL2))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeAllAttrsPlusCacheConfig() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByNodeAttribute(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2))
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                .setPluginProviders(autoActivationProvider)
                .setCacheConfiguration(getCacheConfiguration()))
        ) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_1)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider)
                    .setCacheConfiguration(getCacheConfiguration()));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(getConfiguration(NODE_2)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL2))
                    .setPluginProviders(autoActivationProvider)
                    .setCacheConfiguration(getCacheConfiguration()));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testActivationNotMetInMemoryClusterActivationByNodeAttribute() throws Exception {
        String ATTR_VAL3 = "CELL_03";

        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByNodeAttribute(ATTR, Set.of(ATTR_VAL3))
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                        .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                        .setPluginProviders(autoActivationProvider))
        ) {
            startGrid(getConfiguration(NODE_1)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            startGrid(getConfiguration(NODE_2)
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);
        }
    }

    /** */
    @Test
    public void testAlreadyActivatedInMemoryClusterActivationByNodeAttribute() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByNodeAttribute(ATTR, Set.of(ATTR_VAL1))
        );

        try (IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                .setClusterStateOnStart(ACTIVE)
                .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                .setPluginProviders(autoActivationProvider))) {
            assertTrue(lsnrAlreadyAct.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testBaselineNotEmptyPersistenceClusterActivationByNodeAttribute() throws Exception {
        PluginProvider<?> autoActivationProvider = new AutoActivationPluginProvider(
                new ActivateByNodeAttribute(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2))
        );

        try (
                IgniteEx node0 = startGrid(getConfiguration(NODE_0)
                        .setDataStorageConfiguration(getDataStorageConfiguration())
                        .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                        .setPluginProviders(autoActivationProvider))
        ) {
            startGrid(getConfiguration(NODE_1)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            startGrid(getConfiguration(NODE_2)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL2))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);

            node0.cluster().state(INACTIVE);

            stopAllGrids();

            IgniteEx restartedNode0 = startGrid(getConfiguration(NODE_0)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            startGrid(getConfiguration(NODE_1)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL1))
                    .setPluginProviders(autoActivationProvider));

            startGrid(getConfiguration(NODE_2)
                    .setDataStorageConfiguration(getDataStorageConfiguration())
                    .setUserAttributes(Map.of(ATTR, ATTR_VAL2))
                    .setPluginProviders(autoActivationProvider));

            assertTrue(lsnrBaseline.check());
            assertEquals(restartedNode0.cluster().state(), INACTIVE);
        }
    }

    /** */
    @Test
    public void testAssertionActivationByConsistentId() throws Exception {
        assertThrows(
                listeningLog,
                () -> startGrid(getConfiguration(NODE_0)
                        .setPluginProviders(new AutoActivationPluginProvider(new ActivateByConsistentID(null)))),
                IllegalArgumentException.class,
                "requiredNodes must be set"
        );

        assertThrows(
                listeningLog,
                () -> startGrid(getConfiguration(NODE_0)
                        .setPluginProviders(new AutoActivationPluginProvider(new ActivateByNodeAttribute(null, null)))),
                IllegalArgumentException.class,
                "attributeName must be set"
        );

        assertThrows(
                listeningLog,
                () -> startGrid(getConfiguration(NODE_0)
                        .setPluginProviders(new AutoActivationPluginProvider(new ActivateByNodeAttribute("", null)))),
                IllegalArgumentException.class,
                "attributeName must be set"
        );

        assertThrows(
                listeningLog,
                () -> startGrid(getConfiguration(NODE_0)
                    .setPluginProviders(new AutoActivationPluginProvider(new ActivateByNodeAttribute(ATTR, null)))),
                IllegalArgumentException.class,
                "requiredValues must be set"
        );

        assertThrows(
                listeningLog,
                () -> startGrid(getConfiguration(NODE_0)
                        .setPluginProviders(new AutoActivationPluginProvider(new ActivateByNodeAttribute(ATTR, Collections.emptySet())))),
                IllegalArgumentException.class,
                "requiredValues must be set"
        );

        assertThrows(
                listeningLog,
                () -> startGrid(getConfiguration(NODE_0)
                        .setPluginProviders(new AutoActivationPluginProvider(null))),
                IllegalArgumentException.class,
                "Auto activation condition must be set"
        );
    }

    /** */
    @Test
    public void testXmlCfgPersistenceClusterActivationByConsistentId() throws Exception {
        try (
                IgniteEx node0 =
                        startGrid(loadConfiguration("activate-by-consistent-ID/ignite-server-node1.xml")
                                .setGridLogger(listeningLog))
        ) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(loadConfiguration("activate-by-consistent-ID/ignite-server-node2.xml")
                    .setGridLogger(listeningLog));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(loadConfiguration("activate-by-consistent-ID/ignite-server-node3.xml")
                    .setGridLogger(listeningLog));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }

    /** */
    @Test
    public void testXmlCfgPersistenceClusterActivationByNodeAttribute() throws Exception {
        try (
                IgniteEx node0 = startGrid(loadConfiguration("activate-by-node-attribute/ignite-server-node1.xml")
                .setGridLogger(listeningLog))
        ) {
            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(loadConfiguration("activate-by-node-attribute/ignite-server-node2.xml").setGridLogger(listeningLog));

            assertTrue(lsnrActNotMeet.check());
            assertFalse(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), INACTIVE);

            startGrid(loadConfiguration("activate-by-node-attribute/ignite-server-node3.xml").setGridLogger(listeningLog));

            assertTrue(lsnrActMeet.check());
            assertEquals(node0.cluster().state(), ACTIVE);
        }
    }
}
