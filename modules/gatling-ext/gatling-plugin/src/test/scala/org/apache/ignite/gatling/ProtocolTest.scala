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
package org.apache.ignite.gatling

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteClientFixedSizePool
import org.apache.ignite.gatling.protocol.IgniteClientPerThreadPool
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.apache.ignite.internal.IgnitionEx
import org.junit.Test

/**
 * Tests ignite protocol configuration.
 */
class ProtocolTest extends AbstractGatlingTest {

    override protected def beforeTest(): Unit = {
        super.beforeTest()

        ProtocolTest.igniteConfiguration = optimize(getConfiguration())
    }

    @Test
    /** Tests ignite protocol setup with fixed size thin client pool. */
    def thinClientFixedSizePoolConfig(): Unit = runWith(ThinClient)(simulation = classOf[ThinClientFixedSizePoolSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thin client pool creating one client per thread. */
    def thinClientPerThreadPoolConfig(): Unit = runWith(ThinClient)(simulation = classOf[ThinClientPerThreadPoolSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thin client configuration. */
    def thinClientConfig(): Unit = runWith(ThinClient)(simulation = classOf[ThinClientConfigSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thin client configuration. */
    def thinClientConfigPath(): Unit = runWith(ThinClient)(simulation = classOf[ThinClientConfigPathSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thin client configuration and explicit client start. */
    def thinClientConfigExplicitStart(): Unit = runWith(ThinClient)(simulation = classOf[ThinClientConfigExplicitSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thin client instance. */
    def thinClient(): Unit = runWith(ThinClient)(simulation = classOf[ThinClientSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thick client configuration. */
    def thickClientConfig(): Unit = run(simulation = classOf[ThickClientConfigSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thick client configuration. */
    def thickClientConfigPath(): Unit = run(simulation = classOf[ThickClientConfigPathSimulation].getName)

    @Test
    /** Tests ignite protocol setup with thick client instance. */
    def thickClient(): Unit = runWith(NodeApi)(simulation = classOf[ThickClientSimulation].getName)
}

private object ProtocolTest {
    /** Ignite node configuration to be passed from test to simulation. */
    var igniteConfiguration: IgniteConfiguration = new IgniteConfiguration()
}

abstract class BaseProtocolSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "TEST-CACHE"

    /** Actions to execute. */
    private val actions: ChainBuilder = ignite(
        startIgniteApi as "start",
        getOrCreateCache(cache) as "create",
        put[String, Int](cache, "#{key}", "#{value}"),
        get[String, Int](cache, "#{key}") check entries[String, Int].transform(_.value).is("#{value}"),
        closeIgniteApi as "close"
    )

    private val scn = scenario("protocol")
        .feed(feeder)
        .exec(actions)

    after {
        protocol.close()
    }

    setUp(scn.inject(atOnceUsers(8)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}

/**
 */
class ThinClientFixedSizePoolSimulation extends BaseProtocolSimulation {
    override lazy val protocol: IgniteProtocol =
        igniteProtocol.clientPool(
            new IgniteClientFixedSizePool(
                new ClientConfiguration()
                    .setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}"),
                2
            )
        )
}

/**
 */
class ThinClientPerThreadPoolSimulation extends BaseProtocolSimulation {
    override lazy val protocol: IgniteProtocol =
        igniteProtocol.clientPool(
            new IgniteClientPerThreadPool(
                new ClientConfiguration()
                    .setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}")
            )
        )
}

/**
 */
class ThinClientConfigSimulation extends BaseProtocolSimulation {
    override def protocol: IgniteProtocol =
        igniteProtocol.clientCfg(
            new ClientConfiguration()
                .setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}")
        )
}

/**
 */
class ThinClientConfigPathSimulation extends BaseProtocolSimulation {
    override def protocol: IgniteProtocol =
        igniteProtocol.clientCfgPath(
            Thread.currentThread().getContextClassLoader.getResource("ignite-thin-config.xml")
        )
}

/**
 */
class ThickClientConfigPathSimulation extends BaseProtocolSimulation {
    override def protocol: IgniteProtocol =
        igniteProtocol.igniteCfgPath(
            Thread.currentThread().getContextClassLoader.getResource("ignite-config.xml").getPath
        )
}

/**
 */
class ThickClientConfigSimulation extends BaseProtocolSimulation {
    override def protocol: IgniteProtocol =
        igniteProtocol.igniteCfg(
            ProtocolTest.igniteConfiguration
                .setClientMode(true)
                .setIgniteInstanceName("client")
        )
}

/**
 */
class ThinClientConfigExplicitSimulation extends BaseProtocolSimulation {
    override def protocol: IgniteProtocol =
        igniteProtocol
            .clientCfg(
                new ClientConfiguration()
                    .setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}")
            )
            .withExplicitClientStart
}

/**
 */
class ThinClientSimulation extends BaseProtocolSimulation {
    override lazy val protocol: IgniteProtocol =
        igniteProtocol.client(
            Ignition.startClient(
                new ClientConfiguration()
                    .setAddresses(s"${System.getProperty("host")}:${System.getProperty("port")}")
            )
        )
}

/**
 */
class ThickClientSimulation extends BaseProtocolSimulation {
    override def protocol: IgniteProtocol =
        igniteProtocol.ignite(
            IgnitionEx.allGrids().get(1)
        )
}
