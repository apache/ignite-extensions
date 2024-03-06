/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling

import java.util.concurrent.atomic.AtomicInteger

import javax.cache.processor.MutableEntry

import scala.language.postfixOps

import com.sbt.ignite.gatling.InvokeTest.getValue
import com.sbt.ignite.gatling.Predef._
import com.sbt.ignite.gatling.Predef.group
import com.sbt.ignite.gatling.utils.AbstractGatlingTest
import com.sbt.ignite.gatling.utils.IgniteClientApi.NodeApi
import com.sbt.ignite.gatling.utils.IgniteSupport
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.session.ExpressionSuccessWrapper
import org.junit.Test

/**
 * Tests invoke entry processor.
 */
class InvokeTest extends AbstractGatlingTest {
    @Test
    /** Tests entry processor without arguments. */
    def noArgs(): Unit = runWith(NodeApi)(simulation = classOf[InvokeSimulation].getName)

    @Test
    /** Tests entry processor with additional arguments passed. */
    def withArgs(): Unit = runWith(NodeApi)(simulation = classOf[InvokeArgsSimulation].getName)
}

object InvokeTest {
    /** Helper to extract the fed value from session */
    val getValue = "#{value}"
}

/**
 * invoke entry processor without arguments simulation.
 */
class InvokeSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val atomicCache = "ATOMIC-TEST-CACHE"
    private val transactionalCache = "TRANSACTIONAL-TEST-CACHE"

    private val key = "1"
    private val value = new AtomicInteger(0)

    private val syncOperations = ignite(
        put[String, Int](transactionalCache, key, getValue),
        invoke[String, Int, Unit](transactionalCache, key) { e: MutableEntry[String, Int] =>
            e.setValue(-e.getValue)
        },
        get[String, Int](transactionalCache, key)
            check (
                entries[String, Int].validate((e: Entry[String, Int], s: Session) => e.value == -s("value").as[Int]),
                entries[String, Int].transform(-_.value).is(getValue)
            )
    )

    private val asyncOperations = ignite(
        put[String, Int](atomicCache, key, getValue) async,
        invoke[String, Int, Unit](atomicCache, key) { e: MutableEntry[String, Int] =>
            e.setValue(-e.getValue)
        } async,
        get[String, Int](atomicCache, key)
            check (
                entries[String, Int].validate((e: Entry[String, Int], s: Session) => e.value == -s("value").as[Int]),
                entries[String, Int].transform(-_.value).is(getValue)
            ) async
    )

    private val scn = scenario("invoke")
        .feed(Iterator.continually(Map("value" -> value.incrementAndGet())))
        .ignite(
            getOrCreateCache(atomicCache),
            group("run outside of transaction")(
                asyncOperations
            ),
            getOrCreateCache(transactionalCache) atomicity TRANSACTIONAL,
            tx run (
                syncOperations
            ) as "run in transaction"
        )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}

/**
 * invoke entry processor with arguments simulation.
 */
class InvokeArgsSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "TEST-CACHE"

    private val key = "1"
    private val value = new AtomicInteger(0)

    private val syncOperations = ignite(
        put[String, Int](cache, key, getValue),
        invoke[String, Int, Unit](cache, key).args(8.expressionSuccess) { (e: MutableEntry[String, Int], args: Seq[Any]) =>
            e.setValue(e.getValue * args.head.asInstanceOf[Integer])
        },
        get[String, Int](cache, key)
            check (
                entries[String, Int].validate((e: Entry[String, Int], s: Session) =>
                    e.value == 8 * s("value").as[Int]
                ),
                entries[String, Int].transform(_.value / 8).is(getValue)
            )
    )

    private val asyncOperations = ignite(
        put[String, Int](cache, key, getValue) async,
        invoke[String, Int, Unit](cache, key).args(8.expressionSuccess) { (e: MutableEntry[String, Int], args: Seq[Any]) =>
            e.setValue(e.getValue * args.head.asInstanceOf[Integer])
        } async,
        get[String, Int](cache, key)
            check (
                entries[String, Int].validate((e: Entry[String, Int], s: Session) =>
                    e.value == 8 * s("value").as[Int]
                ),
                entries[String, Int].transform(_.value / 8).is(getValue)
            ) async
    )

    private val scn = scenario("invoke")
        .feed(Iterator.continually(Map("value" -> value.incrementAndGet())))
        .ignite(
            getOrCreateCache(cache) atomicity TRANSACTIONAL,
            group("run outside of transaction")(
                asyncOperations
            ),
            tx run (
                syncOperations
            ) as "run in transaction"
        )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
