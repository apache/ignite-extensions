/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.examples

import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder

/**
 * Sample application running the Gatling simulation.
 */
object GatlingRunner {
    /**
     * @param args Command line arguments.
     */
    def main(args: Array[String]): Unit = {
        val simulationClass = classOf[BasicSimulation].getName

        val props = new GatlingPropertiesBuilder
        props.simulationClass(simulationClass)

        Gatling.fromMap(props.build)
    }
}
