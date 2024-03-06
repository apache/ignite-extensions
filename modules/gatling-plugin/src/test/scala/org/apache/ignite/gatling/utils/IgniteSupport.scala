/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.utils

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.SortedMap
import scala.util.Try

import org.apache.ignite.gatling.Predef.igniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol
import io.gatling.core.feeder.Feeder
import io.gatling.core.feeder.Record
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.internal.IgnitionEx

/**
 * Gatling simulation mixin to support unit tests.
 */
trait IgniteSupport {
    /**
     * Helper feeder class generating long key and long value pairs.
     */
    class IntPairsFeeder extends Feeder[Int] {
        private val atomicInteger: AtomicInteger = new AtomicInteger(0)

        override def hasNext: Boolean = true

        override def next(): Record[Int] =
            Map("key" -> atomicInteger.incrementAndGet(), "value" -> atomicInteger.incrementAndGet())
    }

    /**
     * Helper feeder class generating batches of long key and long value pairs.
     */
    class BatchFeeder extends Feeder[SortedMap[Int, Int]] {
        private val atomicInteger: AtomicInteger = new AtomicInteger(0)

        override def hasNext: Boolean = true

        override def next(): Record[SortedMap[Int, Int]] =
            Map(
                "batch" -> SortedMap(
                    atomicInteger.incrementAndGet() -> atomicInteger.incrementAndGet(),
                    atomicInteger.incrementAndGet() -> atomicInteger.incrementAndGet(),
                    atomicInteger.incrementAndGet() -> atomicInteger.incrementAndGet()
                )
            )
    }

    /** Default feeder for unit tests. */
    protected val feeder: IntPairsFeeder = new IntPairsFeeder()

    /**
     * Setup Ignite protocol for gatling simulation used in unit tests.
     *
     * @return Ignite Protocol instance.
     */
    protected def protocol: IgniteProtocol =
        Option(System.getProperty("host"))
            .flatMap(host => Option(System.getProperty("port")).map(port => (host, port)))
            .map { case (host, port) =>
                Try(new ClientConfiguration().setAddresses(s"$host:$port"))
                    .map(cfg => igniteProtocol.clientCfg(cfg).build)
                    .getOrElse(igniteProtocol.ignite(IgnitionEx.allGrids().get(1)).build)
            }
            .getOrElse(igniteProtocol.ignite(IgnitionEx.allGrids().get(1)).build)
}
