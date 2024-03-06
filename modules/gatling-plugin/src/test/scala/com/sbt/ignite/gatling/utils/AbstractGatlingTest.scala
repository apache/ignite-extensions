/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.utils

import com.sbt.ignite.gatling.utils.IgniteClientApi.IgniteApi
import com.sbt.ignite.gatling.utils.IgniteClientApi.ThinClient
import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder
import org.apache.ignite.internal.client.thin.AbstractThinClientTest
import org.junit.Assert.assertTrue

/**
 * Abstract gatling test.
 */
abstract class AbstractGatlingTest extends AbstractThinClientTest with GatlingSupport {

    override protected def beforeTest(): Unit = {
        super.beforeTest()

        startGrid(0)
    }

    override protected def afterTest(): Unit = {
        stopAllGrids()

        super.afterTest()
    }

    /**
     * Runs simulation with the specified API.
     *
     * @param api ThinApi or NodeApi.
     * @param simulation Class name of simulation.
     */
    protected def runWith(api: IgniteApi)(simulation: String): Unit = {
        if (api == ThinClient) {
            System.setProperty("host", clientHost(grid(0).cluster.localNode))
            System.setProperty("port", String.valueOf(clientPort(grid(0).cluster.localNode)))
        } else {
            System.clearProperty("host")
            System.clearProperty("port")
            startClientGrid(1)
        }

        run(simulation)
    }
}

/**
 */
trait GatlingSupport {
    /**
     * Runs gatling simulation.
     *
     * @param simulation Class name of simulation.
     */
    protected def run(simulation: String): Unit = {
        val gatlingPropertiesBuilder = new GatlingPropertiesBuilder

        gatlingPropertiesBuilder.simulationClass(simulation)
        gatlingPropertiesBuilder.noReports()

        assertTrue("Count of failed gatling events is not zero", Gatling.fromMap(gatlingPropertiesBuilder.build) == 0)
    }
}

/**
 * Types of Ignite API
 */
object IgniteClientApi extends Enumeration {
    /** Type of enum */
    type IgniteApi = Value

    /** Values */
    val ThinClient, NodeApi = Value
}
