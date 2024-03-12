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

import javax.cache.processor.MutableEntry

import scala.collection.SortedMap
import scala.collection.SortedSet
import scala.language.postfixOps

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.session.ExpressionSuccessWrapper
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.InvokeAllTest.getBatch
import org.apache.ignite.gatling.InvokeAllTest.testCache1
import org.apache.ignite.gatling.InvokeAllTest.testCache2
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests invokeAll.
 */
class InvokeAllTest extends AbstractGatlingTest {
    @Test
    /** Tests simulation with the single entry processor passed to invokeAll. */
    def testSingleProcessor(): Unit = runWith(NodeApi)(classOf[SingleProcessorSimulation].getName)

    @Test
    /** Tests simulation with multiple entry processors passed to invokeAll. */
    def multipleProcessors(): Unit = runWith(NodeApi)(classOf[MultipleProcessorsSimulation].getName)
}

object InvokeAllTest {
    /** First test cache */
    val testCache1 = "TEST-CACHE-1"

    /** Second test cache */
    val testCache2 = "TEST-CACHE-2"

    /** Helper to extract the fed batch from session */
    val getBatch = "#{batch}"
}

/**
 * invokeAll with single entry processor simulation.
 */
class SingleProcessorSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val scn = scenario("invokeAll")
        .feed(new BatchFeeder())
        .ignite(
            getOrCreateCache(testCache1) backups 1 atomicity ATOMIC mode PARTITIONED as "create",
            group("run outside of transaction")(
                asyncOperationsWithCache(testCache1)
            ),
            getOrCreateCache(testCache2) atomicity TRANSACTIONAL mode REPLICATED,
            tx run (
                syncOperationsWithCache(testCache2)
            ) as "run in transaction"
        )

    private def syncOperationsWithCache(cache: String) = ignite(
        putAll[Int, Int](cache, getBatch),
        invokeAll[Int, Int, Unit](cache, SortedSet(1, 3, 5).expressionSuccess) { e: MutableEntry[Int, Int] =>
            e.setValue(-e.getValue)
        },
        invokeAll[Int, Int, Unit](cache, SortedSet(1, 5, 3).expressionSuccess).args(3.expressionSuccess) {
            (e: MutableEntry[Int, Int], args: Seq[Any]) =>
                e.setValue(e.getValue * args.head.asInstanceOf[Integer])
        },
        getAll[Int, Int](cache, SortedSet(1, 3, 5))
            .check(
                entries[Int, Int].findAll.validate((entries: Seq[Entry[Int, Int]], _: Session) =>
                    entries.forall(e => e.value == -3 * (e.key + 1))
                )
            ) as "getAll"
    )

    private def asyncOperationsWithCache(cache: String) = ignite(
        putAll[Int, Int](cache, getBatch) async,
        invokeAll[Int, Int, Unit](cache, SortedSet(1, 3, 5).expressionSuccess) { e: MutableEntry[Int, Int] =>
            e.setValue(-e.getValue)
        } async,
        invokeAll[Int, Int, Unit](cache, SortedSet(1, 5, 3).expressionSuccess).args(3.expressionSuccess) {
            (e: MutableEntry[Int, Int], args: Seq[Any]) =>
                e.setValue(e.getValue * args.head.asInstanceOf[Integer])
        } async,
        getAll[Int, Int](cache, SortedSet(1, 3, 5))
            .check(
                entries[Int, Int].findAll.validate((entries: Seq[Entry[Int, Int]], _: Session) =>
                    entries.forall(e => e.value == -3 * (e.key + 1))
                )
            )
            .async as "getAll"
    )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}

/** Entry processor with arguments. */
private class EntryProcessor extends CacheEntryProcessor[Int, Int, Unit]() {
    override def process(e: MutableEntry[Int, Int], args: Object*): Unit =
        e.setValue(e.getValue * args.toList.head.asInstanceOf[Integer])
}

/**
 * invokeAll with multiple entry processors simulation.
 */
class MultipleProcessorsSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val scn = scenario("invokeAll")
        .feed(new BatchFeeder())
        .ignite(
            getOrCreateCache(testCache1) backups 1 atomicity ATOMIC mode PARTITIONED as "create",
            group("run outside of transaction")(
                asyncOperationsWithCache(testCache1)
            ),
            getOrCreateCache(testCache2) atomicity TRANSACTIONAL mode REPLICATED,
            tx run (
                syncOperationsWithCache(testCache2)
            ) as "run in transaction"
        )

    private def syncOperationsWithCache(cache: String) = ignite(
        putAll[Int, Int](cache, getBatch),
        invokeAll[Int, Int, Unit](
            cache,
            map = SortedMap(
                1 -> new EntryProcessor(),
                3 -> new EntryProcessor()
            )
        ).args(2.expressionSuccess),
        getAll[Int, Int](cache, SortedSet(1, 3))
            .check(
                entries[Int, Int].findAll.validate((entries: Seq[Entry[Int, Int]], _: Session) =>
                    entries.forall(e => e.value == 2 * (e.key + 1))
                )
            )
    )

    private def asyncOperationsWithCache(cache: String) = ignite(
        putAll[Int, Int](cache, getBatch) async,
        invokeAll[Int, Int, Unit](
            cache,
            map = SortedMap(
                1 -> new EntryProcessor(),
                3 -> new EntryProcessor()
            )
        ).args(2.expressionSuccess) async,
        getAll[Int, Int](cache, SortedSet(1, 3))
            .check(
                entries[Int, Int].findAll.validate((entries: Seq[Entry[Int, Int]], _: Session) =>
                    entries.forall(e => e.value == 2 * (e.key + 1))
                )
            ) async
    )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
