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

import java.util.concurrent.ThreadLocalRandom

import scala.language.postfixOps

import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteSupport
import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import org.junit.Test

/**
 * Tests async API is prohibited in transaction context.
 */
class TransactionInvalidAsyncOpsTest extends AbstractGatlingTest {
    /**
     * Tests that check is performed on scenario build stage.
     */
    @Test(expected = classOf[IgniteDslInvalidConfigurationException])
    def asyncOp(): Unit = run(classOf[AsyncOpSimulation].getName)

    /**
     * Tests that check is performed on scenario build stage.
     */
    @Test(expected = classOf[IgniteDslInvalidConfigurationException])
    def asyncOpInGroup(): Unit = run(classOf[AsyncOpInGroupSimulation].getName)

    /**
     * Tests that check is performed on scenario build stage.
     */
    @Test(expected = classOf[IgniteDslInvalidConfigurationException])
    def asyncOpInIgnite(): Unit = run(classOf[AsyncOpInIgniteSimulation].getName)

    /**
     * Tests that check is performed in runtime during the scenario execution.
     */
    @Test(expected = classOf[Throwable])
    def asyncOpInControlFlowStatement(): Unit = runWith(NodeApi)(classOf[AsyncOpInControlFlowStatementSimulation].getName)
}

/**
 */
abstract class BaseInvalidTxAsyncSimulation extends Simulation with IgniteSupport {
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
class AsyncOpSimulation extends BaseInvalidTxAsyncSimulation {
    override protected def actions = ignite(
        tx run (
            get("cache", "key") async
        )
    )
}

/**
 */
class AsyncOpInGroupSimulation extends BaseInvalidTxAsyncSimulation {
    override protected def actions = ignite(
        tx run (
            group("")(
                get("cache", "key") async
            )
        )
    )
}

/**
 */
class AsyncOpInIgniteSimulation extends BaseInvalidTxAsyncSimulation {
    override protected def actions = ignite(
        tx run (
            ignite(
                get("cache", "key") async
            )
        )
    )
}

/**
 */
class AsyncOpInControlFlowStatementSimulation extends BaseInvalidTxAsyncSimulation {
    override protected def actions = ignite(
        getOrCreateCache("cache") atomicity TRANSACTIONAL,
        tx run (
            doIfOrElse(session => ThreadLocalRandom.current().nextBoolean())(
                put("cache", "key", "value 1") async
            )(
                put("cache", "key", "value 2") async
            ),
            commit
        )
    )
}
