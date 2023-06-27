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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPluginConfiguration;
import org.apache.ignite.util.GridCommandHandlerMetadataTest;
import org.apache.ignite.util.GridCommandHandlerTest;
import org.apache.ignite.util.KillCommandsControlShTest;
import org.apache.ignite.util.MetricCommandTest;
import org.apache.ignite.util.PerformanceStatisticsCommandTest;
import org.apache.ignite.util.SystemViewCommandTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCommandHandlerTest.class,
    GridCommandHandlerMetadataTest.class,
    KillCommandsControlShTest.class,
    SystemViewCommandTest.class,
    MetricCommandTest.class,
    PerformanceStatisticsCommandTest.class
})
public class OpenApiTestSuite {
    /** */
    @BeforeClass
    public static void setUp() {
        // By default, port range is disabled.
        // Setting up some ports for test to allow starting of several jetty instances on single host.
        OpenApiCommandsRegistryInvokerPluginConfiguration.DFLT_PORT_RANGE = 10;
    }
}
