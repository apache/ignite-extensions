/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.examples

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import com.sbt.ignite.gatling.Predef._
import io.gatling.core.Predef._
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration

/**
 * Basic Ignite Gatling simulation.
 */
class BasicSimulation extends Simulation {
    private val cache = "TEST-CACHE"

    private val c = new AtomicInteger(0)
    private val feeder = Iterator.continually(Map("key" -> c.incrementAndGet(), "value" -> c.incrementAndGet()))

    private val scn = scenario("Basic")
        .feed(feeder)
        .ignite(
            startIgniteApi,
            getOrCreateCache(cache).backups(1) as "Create cache",
            put[Int, Int](cache, "#{key}", "#{value}") as "Put" async,
            get[Int, Int](cache, "#{key}")
                .check(
                    entries[Int, Int].transform(_.value).is("#{value}")
                ) as "Get" async,
            closeIgniteApi
        )

    before {
        Ignition.start()
    }

    after {
        Ignition.allGrids().get(0).close()
    }

    private def protocol = igniteProtocol
        .clientCfg(
            new ClientConfiguration().setAddresses("localhost:10800")
        )
        .withExplicitClientStart

    setUp(
        scn
            .inject(
                constantUsersPerSec(10) during 10.seconds,
                incrementUsersPerSec(1).times(10).eachLevelLasting(1)
            )
    ).protocols(protocol)
        .maxDuration(25.seconds)
        .assertions(global.failedRequests.count.is(0))
}
