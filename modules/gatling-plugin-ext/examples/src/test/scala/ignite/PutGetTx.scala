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
import io.netty.util.internal.ThreadLocalRandom
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.gatling.Predef._

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Basic Ignite Gatling simulation.
 */
class PutGetTx extends Simulation {
    private val cache = "TEST-CACHE"

    private val feeder = Iterator.continually(Map(
        "key" -> ThreadLocalRandom.current().nextInt(10000),
        "value" -> ThreadLocalRandom.current().nextInt()
    ))

    private val scn = scenario("PutGetTxBenchmark")
        .feed(feeder)
        .ignite(
            getOrCreateCache(cache).backups(1) as "Get or create cache",

            tx concurrency PESSIMISTIC isolation REPEATABLE_READ run (

                put[Int, Int](cache, "#{key}", "#{value}") as "txPut",

                get[Int, Int](cache, "#{key}")
                    .check(
                        entries[Int, Int].transform(_.value).is("#{value}")
                    ) as "txGet",

                commit as "txCommit"

            ) as "transaction"
        )

    private val protocol = igniteProtocol
        .igniteCfg(new IgniteConfiguration().setClientMode(true)).build

    after {
        protocol.close()
    }

    setUp(
        scn.inject(
            rampUsersPerSec(0) to 100 during 10.seconds,
            constantUsersPerSec(100) during 20.seconds,
            rampUsersPerSec(100) to 0 during 10.seconds
        )
    ).protocols(protocol)
        .maxDuration(40.seconds)
        .assertions(global.failedRequests.count.is(0))
}
