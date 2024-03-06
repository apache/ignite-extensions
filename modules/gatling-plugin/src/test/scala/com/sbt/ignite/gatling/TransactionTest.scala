/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling

import java.util.concurrent.ThreadLocalRandom

import scala.language.postfixOps

import com.sbt.ignite.gatling.Predef._
import com.sbt.ignite.gatling.utils.AbstractGatlingTest
import com.sbt.ignite.gatling.utils.IgniteClientApi.NodeApi
import com.sbt.ignite.gatling.utils.IgniteClientApi.ThinClient
import com.sbt.ignite.gatling.utils.IgniteSupport
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
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
            doIfOrElse(session => ThreadLocalRandom.current().nextBoolean())(
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
