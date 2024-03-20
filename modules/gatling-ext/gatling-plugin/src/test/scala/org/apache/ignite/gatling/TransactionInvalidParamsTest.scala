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

import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.utils.GatlingSupport
import org.apache.ignite.gatling.utils.IgniteSupport
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
    @Test
    def invalidTxConfig(): Unit =
        expecting[IgniteDslInvalidConfigurationException] {
            run(simulation)
        }

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
    override protected def actions: ChainBuilder = ignite(
        tx isolation READ_COMMITTED run (
            pause(1)
        )
    )
}

/**
 */
class OnlyConcurrencySimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx concurrency OPTIMISTIC run (
            pause(1)
        )
    )
}

/**
 */
class OnlyTimeoutSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx timeout 10L run (
            pause(1)
        )
    )
}

/**
 */
class OnlySizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx size 10 run (
            pause(1)
        )
    )
}

/**
 */
class TimeoutAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx timeout 10L size 10 run (
            pause(1)
        )
    )
}

/**
 */
class ConcurrencyTimeoutAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx concurrency PESSIMISTIC timeout 10L size 10 run (
            pause(1)
        )
    )
}

/**
 */
class IsolationTimeoutAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx isolation REPEATABLE_READ timeout 10L size 10 run (
            pause(1)
        )
    )
}

/**
 */
class ConcurrencyAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx concurrency PESSIMISTIC size 10 run (
            pause(1)
        )
    )
}

/**
 */
class IsolationAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx isolation REPEATABLE_READ size 10 run (
            pause(1)
        )
    )
}

/**
 */
class ConcurrencyAndTimeoutSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx concurrency PESSIMISTIC timeout 10L run (
            pause(1)
        )
    )
}

/**
 */
class IsolationAndTimeoutSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx isolation REPEATABLE_READ timeout 10L run (
            pause(1)
        )
    )
}

/**
 */
class IsolationConcurrencyAndSizeSimulation extends BaseInvalidTxConfigSimulation {
    override protected def actions: ChainBuilder = ignite(
        tx isolation REPEATABLE_READ concurrency OPTIMISTIC size 2 run (
            pause(1)
        )
    )
}
