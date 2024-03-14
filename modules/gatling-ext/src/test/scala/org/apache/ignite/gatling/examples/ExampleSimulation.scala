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
package org.apache.ignite.gatling.examples

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import io.gatling.app.Gatling
import io.gatling.core.Predef._
import io.gatling.core.config.GatlingPropertiesBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.examples.ExampleSimulationRunner.cache

/**
 * Basic Ignite Gatling simulation.
 */
class ExampleSimulation extends Simulation {

    private val c = new AtomicInteger(0)
    private val feeder = Iterator.continually(Map("key" -> c.incrementAndGet(), "value" -> c.incrementAndGet()))

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite(
            put[Int, Int](cache, "#{key}", "#{value}") as "Put" async,
            get[Int, Int](cache, "#{key}")
                .check(
                    entries[Int, Int].transform(_.value).is("#{value}")
                ) as "Get" async
        )

    private def protocol = igniteProtocol
        .clientCfg(
            new ClientConfiguration().setAddresses("localhost:10800")
        )

    setUp(
        scn
            .inject(
                constantUsersPerSec(10) during 10.seconds,
                incrementUsersPerSec(1).times(10).eachLevelLasting(1).startingFrom(10)
            )
    ).protocols(protocol)
        .maxDuration(25.seconds)
        .assertions(global.failedRequests.count.is(0))
}

/**
 * Sample application running the Gatling simulation.
 */
object ExampleSimulationRunner {
    /** */
    val cache = "TEST-CACHE"

    /**
     * @param args Command line arguments.
     */
    def main(args: Array[String]): Unit = {
        val ignite = Ignition.start()

        ignite.createCache(cache)

        val simulationClass = classOf[ExampleSimulation].getName

        val props = new GatlingPropertiesBuilder
        props.simulationClass(simulationClass)

        Gatling.fromMap(props.build)

        ignite.close()
    }
}
