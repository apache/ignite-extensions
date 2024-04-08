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

import java.util.concurrent.ThreadLocalRandom

import scala.language.postfixOps

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests transactions with the correct parameters.
 */
class TransactionTest extends AbstractGatlingTest {
    /** Class name of simulation */
    val simulation: String = classOf[TransactionSimulation].getName

    /** Runs simulation with thin client. */
    @Test
    def thinClient(): Unit = runWith(ThinClient)(simulation)

    /** Runs simulation with thick client. */
    @Test
    def thickClient(): Unit = runWith(NodeApi)(simulation)
}

/**
 * Commit and rollback simulation.
 */
class TransactionSimulation extends Simulation with StrictLogging with IgniteSupport {
    private val key = "key"
    private val value = "value"

    private val cache = "TEST-CACHE"

    private val commitTx = ignite(
        tx concurrency PESSIMISTIC isolation READ_COMMITTED timeout 3000 size 1 run (
            get[Int, Int](cache, s"#{$key}") check entries[Int, Int].notExists,
            put[Int, Int](cache, s"#{$key}", s"#{$value}"),
            commit as "commit"
        ) as "commit transaction",
        get[Int, Any](cache, s"#{$key}")
            check (
                mapResult[Int, Any].saveAs("C"),
                mapResult[Int, Any].validate((m: Map[Int, Any], session: Session) =>
                    m(session(key).as[Int]) == session(value).as[Int]
                )
            ) async
    )

    private val rollbackTx = ignite(
        tx concurrency OPTIMISTIC isolation REPEATABLE_READ run (
            put[Int, Int](cache, s"#{$key}", s"#{$value}"),
            rollback as "rollback"
        ),
        get[Int, Any](cache, s"#{$key}")
            check (
                mapResult[Int, Any].saveAs("R"),
                mapResult[Int, Any].validate { (m: Map[Int, Any], session: Session) =>
                    logger.info(m.toString)
                    m(session(key).as[Int]) == null
                }
            ) async
    )

    private val autoRollbackTx = ignite(
        tx run (
            put[Int, Int](cache, s"#{$key}", s"#{$value}")
        ),
        get[Int, Any](cache, s"#{$key}")
            check (
                mapResult[Int, Any].saveAs("R"),
                mapResult[Int, Any].validate { (m: Map[Int, Any], session: Session) =>
                    logger.info(m.toString)
                    m(session(key).as[Int]) == null
                }
            ) async
    )

    private val controlFlowOperationsTx = ignite(
        tx isolation REPEATABLE_READ concurrency OPTIMISTIC timeout 3000 run (
            get[Int, Int](cache, s"#{$key}"),
            doIfOrElse(_ => ThreadLocalRandom.current().nextBoolean())(
                put[Int, Int](cache, s"#{$key}", -1)
            )(
                put[Int, Int](cache, s"#{$key}", -100)
            ),
            randomSwitch(
                10.0 -> rollback,
                90.0 -> commit
            )
        ) as "transaction with allowed control flow operations"
    )

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite(
            startIgniteApi,
            getOrCreateCache(cache) backups 0 atomicity TRANSACTIONAL,
            rollbackTx,
            autoRollbackTx,
            commitTx,
            controlFlowOperationsTx,
            closeIgniteApi
        )

    setUp(scn.inject(constantUsersPerSec(10).during(5)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
