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
package ignite

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import io.netty.util.internal.ThreadLocalRandom
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteClientPerThreadPool
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Ignite gatling simulation example demonstrating:
 *
 * - Test scenario defined via the Ignite operations DSL.
 * - Use async variant of Ignite APIs.
 * - Ignite gatling protocol configured with thin Ignite client pool creating one client per thread.
 * - Generate maximum possible load using fixed number of threads (16).
 */
class DslPutAsyncThinPool extends Simulation {
    // Start server ignite node before the simulation start.
    //
    // It is started at the same JVM the simulation would run just for simplicity.
    // In the real world testing conditions server ignite cluster nodes forming the cluster
    // under test would be started in some other way, outside the simulation class.
    private val server: Ignite = Ignition.start()

    // Ensure Gatling engine creates enough threads.
    System.setProperty("io.netty.eventLoopThreads", "16")

    val feeder: Feeder[Int] = Iterator.continually(Map(
        "key" -> ThreadLocalRandom.current().nextInt(10000),
        "value" -> ThreadLocalRandom.current().nextInt()
    ))

    val scn: ScenarioBuilder = scenario("PutAsyncThinPool")
        .feed(feeder)
        .ignite(
            getOrCreateCache("cache") as "Get or create cache",

            put[Int, Int]("cache", "#{key}", "#{value}") as "Put" async,
        )

    val pool = new IgniteClientPerThreadPool(
        new ClientConfiguration().setAddresses("localhost:10800")
    )

    val protocol: IgniteProtocol = igniteProtocol.clientPool(pool)

    after {
        pool.close()

        server.close()
    }

    setUp(scn.inject(
        // Generate maximum possible load running 16 threads
        constantConcurrentUsers(16) during 30.seconds
    )).protocols(protocol)
        .assertions(global.failedRequests.count.is(0))
}
