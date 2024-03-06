/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock

import javax.cache.processor.MutableEntry

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.apache.ignite.gatling.LockTest.getValue
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteSupport
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import org.junit.Test

/**
 * Tests that locks work as expected.
 */
class LockTest extends AbstractGatlingTest {
    /**
     */
    @Test
    def validLockTest(): Unit = runWith(NodeApi)(simulation = classOf[LockSimulation].getName)

    /**
     */
    @Test(expected = classOf[Throwable])
    def invalidLockWithAsyncOpsTest(): Unit = runWith(NodeApi)(simulation = classOf[LockWithAsyncOpSimulation].getName)
}

object LockTest {
    /** Helper to extract the fed value from session */
    val getValue = "#{value}"
}

/**
 * Tests that locks work as expected and invoke accepts lambda.
 */
class LockSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "TEST-CACHE"

    private val key = "1"
    private val value = new AtomicInteger(0)

    private val scn = scenario("LockedInvoke")
        .feed(Iterator.continually(Map("value" -> value.incrementAndGet())))
        .ignite(
            getOrCreateCache(cache) atomicity TRANSACTIONAL,
            lock(cache, key)
                check entries[String, Lock].transform(_.value).saveAs("lock"),
            put[String, Int](cache, key, getValue),
            invoke[String, Int, Unit](cache, key) { e: MutableEntry[String, Int] =>
                e.setValue(-e.getValue)
            },
            get[String, Int](cache, key)
                check (
                    entries[String, Int].validate((e: Entry[String, Int], s: Session) =>
                        e.value == -s("value").as[Int]
                    ),
                    entries[String, Int].transform(-_.value).is(getValue)
                ),
            unlock(cache, "#{lock}"),
            closeIgniteApi
        )

    setUp(
        scn.inject(
            rampUsers(120).during(1.second)
        )
    ).protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}

/**
 */
class LockWithAsyncOpSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "TEST-CACHE"

    private val key = "1"
    private val value = new AtomicInteger(0)

    private val scn = scenario("LockedInvoke")
        .feed(Iterator.continually(Map("value" -> value.incrementAndGet())))
        .ignite(
            getOrCreateCache(cache) atomicity TRANSACTIONAL,
            lock(cache, key)
                check entries[String, Lock].transform(_.value).saveAs("lock"),
            put[String, Int](cache, key, getValue) async,
            unlock(cache, "#{lock}"),
            closeIgniteApi
        )

    setUp(
        scn.inject(
            atOnceUsers(1)
        )
    ).protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
