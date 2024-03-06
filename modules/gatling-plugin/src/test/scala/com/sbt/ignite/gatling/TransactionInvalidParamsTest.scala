/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling

import com.sbt.ignite.gatling.Predef._
import com.sbt.ignite.gatling.utils.GatlingSupport
import com.sbt.ignite.gatling.utils.IgniteSupport
import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
 * Tests invalid combinations of transaction parameters.
 *
 * @param simulation Simulation class name.
 */
@RunWith(classOf[Parameterized])
class TransactionInvalidParamsTest(val simulation: String) extends GatlingSupport {
    /**
     */
    @Test(expected = classOf[IgniteDslInvalidConfigurationException])
    def invalidTxConfig(): Unit = run(simulation)
}

object TransactionInvalidParamsTest {
    /** @return simulation class names. */
    @Parameterized.Parameters(name = "simulation={0}")
    def data: java.util.Collection[Array[String]] = {
        val list = new java.util.ArrayList[Array[String]]()

        Seq(
            classOf[OnlyIsolationSimulation],
            classOf[OnlyIsolationSimulation],
            classOf[OnlyTimeoutSimulation],
            classOf[OnlySizeSimulation],
            classOf[TimeoutAndSizeSimulation],
            classOf[IsolationTimeoutAndSizeSimulation],
            classOf[ConcurrencyTimeoutAndSizeSimulation],
            classOf[ConcurrencyAndSizeSimulation],
            classOf[ConcurrencyAndTimeoutSimulation],
            classOf[IsolationAndSizeSimulation],
            classOf[IsolationAndTimeoutSimulation],
            classOf[IsolationConcurrencyAndSizeSimulation]
        ).foreach(c =>
            list.add(Array(c.getName))
        )

        list
    }
}

/**
 */
abstract class BaseInvalidTxConfigSimulation extends Simulation with IgniteSupport {
    /** @return actions */
    protected def actions: ChainBuilder

    setUp(scenario("Tx").ignite(actions).inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(1)
        )
}

/**
 */
class OnlyIsolationSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx isolation READ_COMMITTED run (
            pause(1)
        )
    )
}

/**
 */
class OnlyConcurrencySimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx concurrency OPTIMISTIC run (
            pause(1)
        )
    )
}

/**
 */
class OnlyTimeoutSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx timeout 10L run (
            pause(1)
        )
    )
}

/**
 */
class OnlySizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx size 10 run (
            pause(1)
        )
    )
}

/**
 */
class TimeoutAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx timeout 10L size 10 run (
            pause(1)
        )
    )
}

/**
 */
class ConcurrencyTimeoutAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx concurrency PESSIMISTIC timeout 10L size 10 run (
            pause(1)
        )
    )
}

/**
 */
class IsolationTimeoutAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx isolation REPEATABLE_READ timeout 10L size 10 run (
            pause(1)
        )
    )
}

/**
 */
class ConcurrencyAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx concurrency PESSIMISTIC size 10 run (
            pause(1)
        )
    )
}

/**
 */
class IsolationAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx isolation REPEATABLE_READ size 10 run (
            pause(1)
        )
    )
}

/**
 */
class ConcurrencyAndTimeoutSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx concurrency PESSIMISTIC timeout 10L run (
            pause(1)
        )
    )
}

/**
 */
class IsolationAndTimeoutSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx isolation REPEATABLE_READ timeout 10L run (
            pause(1)
        )
    )
}

/**
 */
class IsolationConcurrencyAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions = ignite(
        tx isolation REPEATABLE_READ concurrency OPTIMISTIC size 2 run (
            pause(1)
        )
    )
}
