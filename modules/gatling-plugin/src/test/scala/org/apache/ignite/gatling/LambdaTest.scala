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

import scala.language.postfixOps

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.session.Session
import org.apache.ignite.Ignite
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.gatling.LambdaTest.testCache
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests an arbitrary lambda executing Ignite operations not exposed via the DSL.
 */
class LambdaTest extends AbstractGatlingTest {
    /** Runs simulation with thin client. */
    @Test
    def thinClient(): Unit = runWith(ThinClient)(classOf[IgniteClientLambdaSimulation].getName)

    /** Runs simulation with thick client. */
    @Test
    def thickClient(): Unit = runWith(NodeApi)(classOf[IgniteLambdaSimulation].getName)
}

object LambdaTest {
    /** Test cache */
    val testCache = "test-cache"
}

/**
 */
class IgniteLambdaSimulation extends Simulation with IgniteSupport with StrictLogging {

    private val scn = scenario("scenario")
        .feed(feeder)
        .ignite { ignite: Ignite =>
            ignite.createCache[Int, Int](testCache)
        }
        .ignite { (ignite: Ignite, session: Session) =>
            val cache = ignite.cache[Int, Int](testCache)

            cache.put(session("key").as[Int], session("value").as[Int])

            val value = cache.get(session("key").as[Int])

            if (value != session("value").as[Int]) {
                throw new RuntimeException("get returns not the value which was put")
            }
        }
        .ignite({ (ignite: Ignite, session: Session) =>
            val cache = ignite.cache[Int, Int](testCache)

            cache.put(session("key").as[Int], session("value").as[Int])

            val value = cache.get(session("key").as[Int])

            if (value != session("value").as[Int]) {
                throw new RuntimeException("get returns not the value which was put")
            }
        } as "named")

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}

/**
 */
class IgniteClientLambdaSimulation extends Simulation with IgniteSupport with StrictLogging {

    private val igniteClientOperations = (ignite: IgniteClient, session: Session) => {
        val cache = ignite.cache[Int, Int](testCache)

        cache.put(session("key").as[Int], session("value").as[Int])

        val value = cache.get(session("key").as[Int])

        // simulate error in lambda
        throw new RuntimeException("check failed in lambda")
    }

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite { ignite: IgniteClient =>
            ignite.createCache[Int, Int](testCache)
        }
        .exec {
            igniteClientOperations as "named"
        }
        .exec {
            igniteClientOperations
        }

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(2)
        )
}
