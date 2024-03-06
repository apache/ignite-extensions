/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling

import scala.language.postfixOps

import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.PutGetTest.getKey
import org.apache.ignite.gatling.PutGetTest.getValue
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.Predef._
import org.junit.Test

/**
 * Tests Put/Get/Remove key-value operations.
 */
class PutGetTest extends AbstractGatlingTest {
    /** Class name of simulation */
    val simulation: String = classOf[PutGetSimulation].getName

    /** Runs simulation with thin client. */
    @Test
    def thinClient(): Unit = runWith(ThinClient)(simulation)

    /** Runs simulation with thick client. */
    @Test
    def thickClient(): Unit = runWith(NodeApi)(simulation)
}

object PutGetTest {
    /** Helper to extract the fed value from session */
    val getKey = "#{key}"

    /** Helper to extract the fed value from session */
    val getValue = "#{value}"
}

/**
 */
class PutGetSimulation extends Simulation with IgniteSupport with StrictLogging {

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite(
            getOrCreateCache("TEST-CACHE-1") backups 1 atomicity ATOMIC mode PARTITIONED as "create",
            group("run outside of transaction")(
                asyncOperationsWithCache("TEST-CACHE-1")
            ),
            getOrCreateCache("TEST-CACHE-2") atomicity TRANSACTIONAL mode REPLICATED,
            tx run (
                syncOperationsWithCache("TEST-CACHE-2")
            ) as "run in transaction"
        )

    private def syncOperationsWithCache(cache: String) = ignite(
        put[Int, Int](cache, _ => (100, 101).success) as "put100",
        get[Int, Int](cache, key = 100) check entries[Int, Int].count.is(1) as "get100",
        put[Int, Int](cache, getKey, getValue) as "put",
        get[Int, Any](cache, key = -2)
            check (
                mapResult[Int, Any].transform(r => r(-2)).isNull,
                entries[Int, Any].count.is(0),
                entries[Int, Any].notExists
            ) as "get absent",
        get[Int, Int](cache, key = getKey)
            check (
                mapResult[Int, Int].saveAs("savedInSession"),
                mapResult[Int, Int].validate((m: Map[Int, Int], s: Session) =>
                    m(s("key").as[Int]) == s("value").as[Int]
                ),
                entries[Int, Int].count.gt(0),
                entries[Int, Int].count.is(1),
                entries[Int, Int],
                entries[Int, Int].find,
                entries[Int, Int].find(0),
                entries[Int, Int].find(0).transform(_.value).is(getValue),
                entries[Int, Int].findAll,
                entries[Int, Int].is(s => s("key").validate[Int].flatMap(k => s("value").validate[Int].map(v => Entry(k, v)))),
                entries[Int, Int].is(Entry(1, 2))
            ) as "get present",
        remove[Int](cache, key = getKey),
        getAndPut[Int, Int](cache, key = getKey, 1000)
            check (
                entries[Int, Int].count.is(0),
                entries[Int, Int].notExists
            ) as "getAndPut removed",
        getAndRemove[Int, Int](cache, key = getKey)
            check (
                entries[Int, Int].count.is(1),
                entries[Int, Int].exists,
                entries[Int, Int].transform(_.value).is(1000)
            ) as "getAndRemove",
        get[Int, Any](cache, key = -2)
            check (
                entries[Int, Any].count.is(0),
                entries[Int, Any].notExists
            ) as "get removed"
    )

    private def asyncOperationsWithCache(cache: String) = ignite(
        put[Int, Int](cache, _ => (100, 101).success) as "put100" async,
        get[Int, Int](cache, key = 100) check entries[Int, Int].count.is(1) as "get100" async,
        put[Int, Int](cache, getKey, getValue) as "put" async,
        get[Int, Any](cache, key = -2)
            check (
                mapResult[Int, Any].transform(r => r(-2)).isNull,
                entries[Int, Any].count.is(0),
                entries[Int, Any].notExists
            ) as "get absent" async,
        get[Int, Int](cache, key = getKey)
            check (
                mapResult[Int, Int].saveAs("savedInSession"),
                mapResult[Int, Int].validate((m: Map[Int, Int], s: Session) =>
                    m(s("key").as[Int]) == s("value").as[Int]
                ),
                entries[Int, Int].count.gt(0),
                entries[Int, Int].count.is(1),
                entries[Int, Int],
                entries[Int, Int].find,
                entries[Int, Int].find(0),
                entries[Int, Int].find(0).transform(_.value).is(getValue),
                entries[Int, Int].findAll,
                entries[Int, Int].is(s => s("key").validate[Int].flatMap(k => s("value").validate[Int].map(v => Entry(k, v)))),
                entries[Int, Int].is(Entry(1, 2))
            ) as "get present" async,
        remove[Int](cache, key = getKey) async,
        getAndPut[Int, Int](cache, key = getKey, 1000)
            check (
                entries[Int, Int].count.is(0),
                entries[Int, Int].notExists
            ) as "getAndPut removed" async,
        getAndRemove[Int, Int](cache, key = getKey)
            check (
                entries[Int, Int].count.is(1),
                entries[Int, Int].exists,
                entries[Int, Int].transform(_.value).is(1000)
            ) as "getAndRemove" async,
        get[Int, Any](cache, key = -2)
            check (
                entries[Int, Any].count.is(0),
                entries[Int, Any].notExists
            ) as "get removed" async
    )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
