/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling

import com.sbt.ignite.gatling.Predef._
import com.sbt.ignite.gatling.Predef.group
import com.sbt.ignite.gatling.api.node.IgniteNodeApi
import com.sbt.ignite.gatling.api.thin.IgniteThinApi
import com.sbt.ignite.gatling.utils.AbstractGatlingTest
import com.sbt.ignite.gatling.utils.IgniteClientApi.NodeApi
import com.sbt.ignite.gatling.utils.IgniteClientApi.ThinClient
import com.sbt.ignite.gatling.utils.IgniteSupport
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.Predef._
import org.apache.ignite.binary.BinaryObject
import org.junit.Test

/**
 * Tests operations with binary objects and low-level access to underlining Ignite API
 * for functionality not exposed via the DSL.
 */
class BinaryTest extends AbstractGatlingTest {
    /** Class name of simulation */
    val simulation: String = classOf[BinarySimulation].getName

    /** Runs simulation with thin client. */
    @Test
    def thinClient(): Unit = runWith(ThinClient)(simulation)

    /** Runs simulation with thick client. */
    @Test
    def thickClient(): Unit = runWith(NodeApi)(simulation)
}

/**
 */
class BinarySimulation extends Simulation with IgniteSupport with StrictLogging {
    private val bootstrapCache = "bootstrap-cache"

    private val scn = scenario("Binary")
        .feed(feeder)
        .ignite(
            getOrCreateCache(bootstrapCache),
            sql(
                bootstrapCache,
                "CREATE TABLE City (id int primary key, name varchar, region varchar) WITH \"cache_name=s.city,value_type=s.city\""
            ),
            group("run outside of transaction")(
                operationsWithTable("s.city")
            ),
            sql(
                bootstrapCache,
                "CREATE TABLE City2 (id int primary key, name varchar, region varchar) WITH \"atomicity=transactional,cache_name=s.city2,value_type=s.city2\""
            ),
            tx run (
                operationsWithTable("s.city2")
            ) as "run in transaction"
        )

    private def operationsWithTable(cache: String) = ignite(
        // Put binary object
        put[Int, BinaryObject](
            cache,
            "#{key}",
            session =>
                session
                    .binaryBuilder()(typeName = cache)
                    .setField("id", session("key").as[Int])
                    .setField("name", "Nsk")
                    .setField("region", "Region")
                    .build()
                    .success
        ).keepBinary,

        // sql select with the affinity awareness
        sql(cache, "SELECT * FROM City WHERE id = ?")
            .args("#{key}")
            .partitions { session =>
                (session.igniteApi match {
                    case IgniteNodeApi(ignite) => List(ignite.affinity(cache).partition(session("key").as[Int]))

                    case IgniteThinApi(_) => List.empty
                }).success
            }
            .check(
                resultSet.count.is(1),
                resultSet.transform(row => row(1)).is("Nsk")
            )
    )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
