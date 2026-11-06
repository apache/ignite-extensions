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

package org.apache.ignite.activation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** Tests {@link AutoActivationPluginProvider}. */
@RunWith(Parameterized.class)
public class AutoActivationTest extends GridCommonAbstractTest {
    /** Activation type. */
    public enum ActivationType {
        /** */
        CONSISTENT_ID,

        /** */
        NODE_ATTRIBUTE
    }

    /** */
    @Parameterized.Parameter
    public ActivationType activationType;

    /** */
    @Parameterized.Parameters(name = "{0}")
    public static List<ActivationType> parameters() {
        return List.of(ActivationType.values());
    }

    /** Listening test logger. */
    private ListeningTestLogger listeningLog;

    /** */
    private final LogListener lsnrAlreadyAct = LogListener
        .matches("Auto activation skipped - cluster already activated").build();

    /** */
    private final LogListener lsnrBaseline = LogListener
        .matches("Auto activation skipped - baseline is not empty").build();

    /** */
    private final LogListener lsnractMet = LogListener
        .matches("Auto activation plugin set cluster state ACTIVE - activation condition meet").build();

    /** */
    private final LogListener lsnractNotMet = LogListener
        .matches("Auto activation skipped - activation condition not met").build();

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

        listeningLog = new ListeningTestLogger(log);

        listeningLog.registerAllListeners(lsnrAlreadyAct, lsnrBaseline, lsnractMet, lsnractNotMet);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();

        listeningLog.clearListeners();

        listeningLog = null;

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
                    cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
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
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100L * 1024 * 1024));
    }

    /** @return IgniteConfiguration from XML. */
    private IgniteConfiguration getConfigurationFromXml(String xmlPath) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("common-ignite-server-node.xml", xmlPath);

        return ctx.getBean(IgniteConfiguration.class).setGridLogger(listeningLog);
    }

    /** */
    private PluginProvider<?> getPluginProvider(Set<String> requiredValues) {
        switch (activationType) {
            case CONSISTENT_ID:
                return new AutoActivationPluginProvider(
                    new ActivateByConsistentID(requiredValues)
                );
            case NODE_ATTRIBUTE:
                return new AutoActivationPluginProvider(
                    new ActivateByNodeAttribute(ATTR, requiredValues)
                );
            default:
                throw new IllegalStateException(
                    "Unknown activation type: " + activationType
                );
        }
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationAllNodes() throws Exception {
        executeTest(3, getPluginProvider(allRequiredValues()), null,
            List.of("actNotMet", "actNotMet", "actMet"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationFirstNodes() throws Exception {
        List<String> assertions = activationType == ActivationType.CONSISTENT_ID
            ? List.of("actNotMet", "actMet", "alreadyAct")
            : List.of("actMet", "alreadyAct", "alreadyAct");

        executeTest(3, getPluginProvider(activationType == ActivationType.CONSISTENT_ID
            ? Set.of(NODE_0, NODE_1) : Set.of(ATTR_VAL1)), null, assertions);
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationLastNode() throws Exception {
        executeTest(3, getPluginProvider(activationType == ActivationType.CONSISTENT_ID
                ? Set.of(NODE_2) : Set.of(ATTR_VAL2)), null,
            List.of("actNotMet", "actNotMet", "actMet"));
    }

    /** */
    @Test
    public void testSuccessfulInMemoryClusterActivationWithCacheConfig() throws Exception {
        executeTest(3, getPluginProvider(allRequiredValues()), "cacheConf",
            List.of("actNotMet", "actNotMet", "actMet"));
    }

    /** */
    @Test
    public void testActivationConditionNotMet() throws Exception {
        executeTest(3, getPluginProvider(activationType == ActivationType.CONSISTENT_ID
                ? Set.of(NODE_3) : Set.of(ATTR_VAL3)), null,
            List.of("actNotMet", "actNotMet", "actNotMet"));
    }

    /** */
    @Test
    public void testAlreadyActivated() throws Exception {
        executeTest(1, getPluginProvider(activationType == ActivationType.CONSISTENT_ID
                ? Set.of(NODE_0) : Set.of(ATTR_VAL1)), "active",
            List.of("alreadyAct"));
    }

    /** */
    @Test
    public void testAlreadyActivatedReadOnly() throws Exception {
        executeTest(1, getPluginProvider(activationType == ActivationType.CONSISTENT_ID
                ? Set.of(NODE_0) : Set.of(ATTR_VAL1)), "activeReadOnly",
            List.of("alreadyActReadOnly"));
    }

    /** */
    @Test
    public void testBaselineNotEmptyPersistenceClusterActivation() throws Exception {
        executeTest(3, getPluginProvider(allRequiredValues()), "dataStorageConf",
            List.of("actNotMet", "actNotMet", "actMet"));

        stopAllGrids();

        executeTest(3, getPluginProvider(allRequiredValues()), "dataStorageConf",
            List.of("baseline", "baseline", "baseline"));
    }

    /** */
    @Test
    public void testActivationConditionNotMetWithClientNode() throws Exception {
        executeTest(3, getPluginProvider(allRequiredValues()), "clientMode",
            List.of("actNotMet", "actNotMet", "actNotMet"));
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

        Pattern missed = Pattern
            .compile(provider.getCondition().getClass().equals(ActivateByConsistentID.class)
                ? "\\(by consistent ID\\)\\. " +
                    "Missing nodes \\[(?:(?=[^]]*" + NODE_1 + ")|(?=[^]]*" + NODE_2 + ")|(?=[^]]*" + NODE_3 + "))[^]]+]"
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
                ? "\\(by consistent ID\\)\\. Missing nodes \\[(?=[^]]*cell-2_node-1)(?=[^]]*cell-1_node-2)[^]]+]"
                : "\\(by node attribute\\)\\. Attribute: " + ATTR + ", Missing values \\[(?=.*CELL_2).+]"))
            .build();

        listeningLog.registerListener(lsnrMissed);

        try (
            IgniteEx node0 =
                startGrid(getConfigurationFromXml(conditionType + "/ignite-server-node1.xml"))
        ) {
            assertion(node0, "actNotMet");

            startGrid(getConfigurationFromXml(conditionType + "/ignite-server-node2.xml"));

            assertion(node0, "actNotMet");

            startGrid(getConfigurationFromXml(conditionType + "/ignite-server-node3.xml"));

            assertion(node0, "actMet");
        }
    }

    /** */
    private void assertion(IgniteEx node0, String assertion) throws InterruptedException {
        switch (assertion) {
            case "actNotMet":
                assertTrue(lsnractNotMet.check(2000));
                assertTrue(lsnrMissed.check(2000));
                assertFalse(lsnractMet.check(2000));
                assertEquals(node0.cluster().state(), INACTIVE);
                break;
            case "actMet":
                assertTrue(lsnractMet.check(2000));
                assertFalse(lsnrAlreadyAct.check(2000));
                assertEquals(node0.cluster().state(), ACTIVE);
                break;
            case "alreadyAct":
                assertTrue(lsnrAlreadyAct.check(2000));
                assertEquals(node0.cluster().state(), ACTIVE);
                break;
            case "alreadyActReadOnly":
                assertTrue(lsnrAlreadyAct.check(2000));
                assertFalse(lsnractMet.check(2000));
                assertEquals(node0.cluster().state(), ACTIVE_READ_ONLY);
                break;
            case "baseline":
                assertTrue(lsnrBaseline.check(2000));
                assertEquals(node0.cluster().state(), INACTIVE);
                break;
            default:
                throw new IllegalArgumentException("Unknown assertion: " + assertion);
        }
    }

    /** */
    private Set<String> allRequiredValues() {
        return activationType == ActivationType.CONSISTENT_ID
            ? nodesConsistentIds
            : Set.of(ATTR_VAL1, ATTR_VAL2);
    }
}
