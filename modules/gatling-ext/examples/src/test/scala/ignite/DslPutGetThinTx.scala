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
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Ignite gatling simulation example demonstrating:
 *
 * - Test scenario defined via the Ignite operations DSL.
 * - Group Ignite operations in transaction.
 * - Ignite gatling protocol configured via URL of the thin Ignite client Spring XML config file.
 * - Generate load with fixed RPS (100).
 *
 * Before run simulation start the Ignite server node manually on the localhost.
 */
class DslPutGetThinTx extends Simulation {
    /**
     * Feeder generating random test data for keys and values.
     */
    val feeder: Feeder[Int] = Iterator.continually(Map(
        "key" -> ThreadLocalRandom.current().nextInt(10000),
        "value" -> ThreadLocalRandom.current().nextInt()
    ))

    /**
     * Testing scenario defined via the Ignite operations DSL.
     *
     * Each operation like transaction start, put, get, commit will be measured and
     * represented in final HTML report separately.
     *
     * Besides, the gatling group will be created for transaction to measure it as a whole.
     */
    val scn: ScenarioBuilder = scenario("PutGetThinTx")
        .feed(feeder)
        .ignite(
            // Create cache if not exists.
            getOrCreateCache("cache") backups 1 atomicity TRANSACTIONAL mode PARTITIONED
                as "getOrCreateCache",

            // Setup and start the transaction.
            tx concurrency PESSIMISTIC isolation REPEATABLE_READ run (

                // Put random value.
                put[Int, Int]("cache", "#{key}", "#{value}") as "txPut",

                // Get and check that it's the same value as was put.
                get[Int, Int]("cache", "#{key}")
                    .check(
                        entries[Int, Int].transform(_.value).is("#{value}")
                    ) as "txGet",

                // Commit the transaction.
                commit as "txCommit"

            ) as "transaction"
        )

    // Create Ignite gatling protocol passing the path to thin client Spring XML configuration.
    val protocol: IgniteProtocol = igniteProtocol
        .clientCfgPath(
            Thread.currentThread().getContextClassLoader.getResource("ignite-thin-config.xml")
        )

    // Destroy the client node after the simulation.
    after {
        protocol.close()
    }

    setUp(
        scn.inject(
            // Generate constant 100 rps load for 30 seconds.
            constantUsersPerSec(100) during 30.seconds,
        )
    ).protocols(protocol)
        .assertions(global.failedRequests.count.is(0))
}
