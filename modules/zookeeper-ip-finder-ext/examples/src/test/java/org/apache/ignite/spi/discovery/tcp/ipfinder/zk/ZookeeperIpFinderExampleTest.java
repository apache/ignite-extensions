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

package org.apache.ignite.spi.discovery.tcp.ipfinder.zk;

import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.CloseableUtils;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

import static org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING;

/** */
public class ZookeeperIpFinderExampleTest extends GridAbstractExamplesTest {
    /** The ZK cluster instance, from curator-test. */
    private TestingCluster zkCluster;

    /** */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        zkCluster = new TestingCluster(1);

        zkCluster.start();

        System.setProperty(PROP_ZK_CONNECTION_STRING, zkCluster.getConnectString());
    }

    /** */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        if (zkCluster != null)
            CloseableUtils.closeQuietly(zkCluster);

        System.clearProperty(PROP_ZK_CONNECTION_STRING);
    }

    /** */
    @Test
    public void testExample() {
        ZookeeperIpFinderExample.main(EMPTY_ARGS);
    }
}
