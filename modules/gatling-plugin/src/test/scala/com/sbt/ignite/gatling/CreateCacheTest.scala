/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling

import com.sbt.ignite.gatling.Predef._
import com.sbt.ignite.gatling.utils.AbstractGatlingTest
import com.sbt.ignite.gatling.utils.IgniteClientApi.NodeApi
import com.sbt.ignite.gatling.utils.IgniteClientApi.ThinClient
import com.sbt.ignite.gatling.utils.IgniteSupport
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.junit.Test

/**
 * Tests various create cache operations.
 */
class CreateCacheTest extends AbstractGatlingTest {
    /** Create cache via thin client and simple DSL parameters. */
    @Test
    def thinClientDSL(): Unit = runWith(ThinClient)(classOf[CreateCacheDSLSimulation].getName)

    /** Create cache via thick client and simple DSL parameters. */
    @Test
    def thickClientDSL(): Unit = runWith(NodeApi)(classOf[CreateCacheDSLSimulation].getName)

    /** Create cache via thin client and full configuration instance. */
    @Test
    def thinClientConfig(): Unit = runWith(ThinClient)(classOf[CreateCacheThinConfigSimulation].getName)

    /** Create cache via thick client and full configuration instance. */
    @Test
    def thickClientConfig(): Unit = runWith(NodeApi)(classOf[CreateCacheThickConfigSimulation].getName)
}

/**
 * Create cache with simple parameters specified via DSL.
 */
class CreateCacheDSLSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "cache"

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite(
            getOrCreateCache(s"$cache-1") as "1",
            getOrCreateCache(s"$cache-2").backups(1) as "2",
            getOrCreateCache(s"$cache-3").atomicity(TRANSACTIONAL) as "3",
            getOrCreateCache(s"$cache-4").mode(PARTITIONED).backups(1) as "4",
            getOrCreateCache(s"$cache-5") backups 1 atomicity ATOMIC mode REPLICATED as "5"
        )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}

/**
 * Create cache using full ClientCacheConfiguration instance.
 */
class CreateCacheThinConfigSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "cache-thin"

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite(
            getOrCreateCache(cache).cfg(new ClientCacheConfiguration())
        )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}

/**
 * Create cache using full CacheConfiguration instance.
 */
class CreateCacheThickConfigSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "cache-thick"

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite(
            getOrCreateCache(cache).cfg(new CacheConfiguration[Int, Int]()) as "create"
        )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
