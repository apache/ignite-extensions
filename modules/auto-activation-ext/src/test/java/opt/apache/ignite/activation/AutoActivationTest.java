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

package opt.apache.ignite.activation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
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
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Tests {@link AutoActivationPluginProvider}.
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
    private LogListener lsnrMissed;

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
    private final String ATTR_VAL3 = "CELL_03";

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
        return super.getConfiguration(igniteInstanceName)
                    .setConsistentId(igniteInstanceName)
                    .setUserAttributes(igniteInstanceName.equals(NODE_2) ? Map.of(ATTR, ATTR_VAL2) : Map.of(ATTR, ATTR_VAL1))
                    .setClusterStateOnStart(INACTIVE)
                    .setGridLogger(listeningLog);
    }

    /** */
    private IgniteConfiguration getConfiguration(String igniteInstanceName, PluginProvider<?> autoActivationProvider,
                                                 String extraCfg) throws Exception {
        IgniteConfiguration cfg = getConfiguration(igniteInstanceName).setPluginProviders(autoActivationProvider);

        if (extraCfg != null) {
            switch (extraCfg) {
                case "cacheConf":
                    cfg.setCacheConfiguration(getCacheConfiguration());
                    break;
                case "clientMode":
                    cfg.setClientMode(igniteInstanceName.equals(NODE_2));
                case "dataStorageConf":
                    cfg.setDataStorageConfiguration(getDataStorageConfiguration());
                    break;
                case "active":
                    cfg.setClusterStateOnStart(ACTIVE);
                    break;
                case "activeReadOnly":
                    cfg.setClusterStateOnStart(ACTIVE_READ_ONLY);
                    break;
            }
        }

        return cfg;
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
    private @NotNull DataRegionConfiguration getDataRegionConfiguration() {
        return new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(100L * 1024 * 1024);
    }

    /** @return CacheConfiguration. */
    private CacheConfiguration<String, Integer> getCacheConfiguration() {
        return new CacheConfiguration<String, Integer>()
                .setName(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(0)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setIndexedTypes(String.class, Integer.class);
    }

    /** @return IgniteConfiguration from XML. */
    private IgniteConfiguration getConfigurationFromXml(String xmlPath) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("common-ignite-server-node.xml", xmlPath);

        return ctx.getBean(IgniteConfiguration.class).setGridLogger(listeningLog);
    }

    /** @return PluginProvider ActivateByConsistentID. */
    private PluginProvider<?> getPluginProvider(Set<String> consistentIds) {
        return new AutoActivationPluginProvider(new ActivateByConsistentID(consistentIds));
    }

    /** @return PluginProvider ActivateByNodeAttribute. */
    private PluginProvider<?> getPluginProvider(String attrName, Set<String> requiredValues) {
        return new AutoActivationPluginProvider(new ActivateByNodeAttribute(attrName, requiredValues));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdAllNodes() throws Exception {
        executeTest(3, getPluginProvider(nodesConsistentIds), null, List.of("actNotMeet", "actNotMeet", "actMeet"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdFirstTwoNodes() throws Exception {
        executeTest(3, getPluginProvider(Set.of(NODE_0, NODE_1)), null, List.of("actNotMeet", "actMeet", "alreadyAct"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdOnlyLastNode() throws Exception {
        executeTest(3, getPluginProvider(Set.of(NODE_2)), null, List.of("actNotMeet", "actNotMeet", "actMeet"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByConsistentIdAllNodesPlusCacheConfig() throws Exception {
        executeTest(3, getPluginProvider(Set.of(NODE_2)), "cacheConf", List.of("actNotMeet", "actNotMeet", "actMeet"));
    }

    /** */
    @Test
    public void testActivationNotMetInMemoryClusterActivationByConsistentId() throws Exception {
        executeTest(3, getPluginProvider(Set.of(NODE_3)), null, List.of("actNotMeet", "actNotMeet", "actNotMeet"));
    }

    /** */
    @Test
    public void testAlreadyActivatedInMemoryClusterActivationByConsistentId() throws Exception {
        executeTest(1, getPluginProvider(Set.of(NODE_0)), "active", List.of("alreadyAct"));
    }

    /** */
    @Test
    public void testAlreadyActivatedInMemoryClusterActivationByConsistentIdActiveReadOnly() throws Exception {
        executeTest(1, getPluginProvider(Set.of(NODE_0)), "activeReadOnly", List.of("alreadyActReadOnly"));
    }

    /** */
    @Test
    public void testBaselineNotEmptyPersistenceClusterActivationByConsistentId() throws Exception {
        executeTest(3, getPluginProvider(nodesConsistentIds),
                "dataStorageConf", List.of("actNotMeet", "actNotMeet", "actMeet"));

        stopAllGrids();

        executeTest(3, getPluginProvider(nodesConsistentIds),
                "dataStorageConf", List.of("baseline", "baseline", "baseline"));
    }

    /** */
    @Test
    public void testActivationConditionByConsistentIdNotMeetWithClientNode() throws Exception {
        executeTest(3, getPluginProvider(nodesConsistentIds), "clientMode",
                List.of("actNotMeet", "actNotMeet", "actNotMeet"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeAllAttrs() throws Exception {
        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2)), null,
                List.of("actNotMeet", "actNotMeet", "actMeet"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeFirstAttr() throws Exception {
        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL1)), null,
                List.of("actMeet", "alreadyAct", "alreadyAct"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeLastAttr() throws Exception {
        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL2)), null,
                List.of("actNotMeet", "actNotMeet", "actMeet"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationByNodeAttributeAllAttrsPlusCacheConfig() throws Exception {
        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2)), "cacheConf",
                List.of("actNotMeet", "actNotMeet", "actMeet"));
    }

    /** */
    @Test
    public void testActivationNotMetInMemoryClusterActivationByNodeAttribute() throws Exception {
        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL3)), null,
                List.of("actNotMeet", "actNotMeet", "actNotMeet"));
    }

    /** */
    @Test
    public void testAlreadyActivatedInMemoryClusterActivationByNodeAttribute() throws Exception {
        executeTest(1, getPluginProvider(ATTR, Set.of(ATTR_VAL1)), "active", List.of("alreadyAct"));
    }

    /** */
    @Test
    public void testBaselineNotEmptyPersistenceClusterActivationByNodeAttribute() throws Exception {
        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2)), "dataStorageConf",
                List.of("actNotMeet", "actNotMeet", "actMeet"));

        stopAllGrids();

        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2)), "dataStorageConf",
                List.of("baseline", "baseline", "baseline"));
    }

    /** */
    @Test
    public void testActivationConditionByNodeAttributeNotMeetWithClientNode() throws Exception {
        executeTest(3, getPluginProvider(ATTR, Set.of(ATTR_VAL1, ATTR_VAL2)), "clientMode",
                List.of("actNotMeet", "actNotMeet", "actNotMeet"));
    }

    /** */
    @Test
    public void testExceptionActivation() {
        executeExceptionTest(ActivateByConsistentID.class, null, null, "requiredNodes must be set");

        executeExceptionTest(ActivateByConsistentID.class, null, Collections.emptySet(), "requiredNodes must be set");

        executeExceptionTest(ActivateByNodeAttribute.class, null, null, "attributeName must be set");

        executeExceptionTest(ActivateByNodeAttribute.class, "", null, "attributeName must be set");

        executeExceptionTest(ActivateByNodeAttribute.class, ATTR, null, "requiredValues must be set");

        executeExceptionTest(ActivateByNodeAttribute.class, ATTR, Collections.emptySet(), "requiredValues must be set");

        executeExceptionTest(null, null, null, "Auto activation condition must be set");
    }

    /** */
    @Test
    public void testXmlCfgPersistenceClusterActivationByConsistentId() throws Exception {
        executeXmlTest("activate-by-consistent-ID");
    }

    /** */
    @Test
    public void testXmlCfgPersistenceClusterActivationByNodeAttribute() throws Exception {
        executeXmlTest("activate-by-node-attribute");
    }

    /** */
    private void executeTest(int nodesCount, PluginProvider<?> autoActivationProvider,
                             String extraCfg, List<String> assertions) throws Exception {
        AutoActivationPluginProvider provider = (AutoActivationPluginProvider)autoActivationProvider;

        log.info("Classs. " + provider.getCondition());
        log.info("Classs. " + autoActivationProvider.copyright());
        log.info("Classs. " + autoActivationProvider.version());
        log.info("Classs. " + autoActivationProvider.toString());
        Pattern missed = Pattern
                .compile(provider.getCondition().getClass().equals(ActivateByConsistentID.class)
                        ? "\\(by consistent ID\\)\\. " +
                            "Missing nodes \\[(?:(?=.*" + NODE_1 + ")|(?=.*" + NODE_2 + ")|(?=.*" + NODE_3 + ")).+]"
                        : "\\(by node attribute\\)\\. Attribute: " + ATTR + ", " +
                            "Missing values \\[(?:(?=.*" + ATTR_VAL2 + ")|(?=.*" + ATTR_VAL3 + ")).+]");

        lsnrMissed = LogListener.matches(missed).build();

        listeningLog.registerListener(lsnrMissed);

        try (IgniteEx node0 = startGrid(getConfiguration(NODE_0, autoActivationProvider, extraCfg))) {
            assertion(node0, assertions.get(0));

            if (nodesCount > 1) {
                for (int i = 1; i < nodesCount; i++) {
                    startGrid(getConfiguration("node_" + i, autoActivationProvider, extraCfg));

                    assertion(node0, assertions.get(i));
                }
            }
        }
    }

    /** */
    private void executeExceptionTest(Class<?> condition, String attributeName,
                                      Set<String> nodesOrAttrValues, String exceptionMessage) {
        assertThrowsAnyCause(
                listeningLog,
                () -> startGrid(getConfiguration(NODE_0)
                        .setPluginProviders(new AutoActivationPluginProvider(
                                (condition == null) ? null : condition == ActivateByConsistentID.class
                                        ? new ActivateByConsistentID(nodesOrAttrValues)
                                        : new ActivateByNodeAttribute(attributeName, nodesOrAttrValues)))),
                IllegalArgumentException.class,
                exceptionMessage
        );
    }

    /** */
    private void executeXmlTest(String conditionType) throws Exception {
        lsnrMissed = LogListener
                .matches(Pattern.compile(conditionType.equals("activate-by-consistent-ID")
                        ? "\\(by consistent ID\\)\\. Missing nodes \\[(?:(?=.*cell-2_node-1)|(?=.*cell-1_node-2)).+]"
                        : "\\(by node attribute\\)\\. Attribute: " + ATTR + ", Missing values \\[(?=.*CELL_2).+]"))
                .build();

        listeningLog.registerListener(lsnrMissed);

        try (
                IgniteEx node0 =
                        startGrid(getConfigurationFromXml(conditionType + "/ignite-server-node1.xml"))
        ) {
            assertion(node0, "actNotMeet");

            startGrid(getConfigurationFromXml(conditionType + "/ignite-server-node2.xml"));

            assertion(node0, "actNotMeet");

            startGrid(getConfigurationFromXml(conditionType + "/ignite-server-node3.xml"));

            assertion(node0, "actMeet");
        }
    }

    /** */
    private void assertion(IgniteEx node0, String assertion) {
        switch (assertion) {
            case "actNotMeet":
                assertTrue(lsnrActNotMeet.check());
                assertTrue(lsnrMissed.check());
                assertFalse(lsnrActMeet.check());
                assertEquals(node0.cluster().state(), INACTIVE);
                break;
            case "actMeet":
                assertTrue(lsnrActMeet.check());
                assertFalse(lsnrAlreadyAct.check());
                assertEquals(node0.cluster().state(), ACTIVE);
                break;
            case "alreadyActFalseAndActMeet":
                assertFalse(lsnrActMeet.check());
            case "alreadyAct":
                assertTrue(lsnrAlreadyAct.check());
                assertEquals(node0.cluster().state(), ACTIVE);
                break;
            case "alreadyActReadOnly":
                assertTrue(lsnrAlreadyAct.check());
                assertFalse(lsnrActMeet.check());
                assertEquals(node0.cluster().state(), ACTIVE_READ_ONLY);
                break;
            case "baseline":
                assertTrue(lsnrBaseline.check());
                assertEquals(node0.cluster().state(), INACTIVE);
                break;
        }
    }
}
