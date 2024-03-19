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
package ignite

import io.gatling.core.Predef._
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.internal.IgniteEx

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Random.javaRandomToRandom

/**
 * Ignite gatling simulation example demonstrating:
 *
 * - Test scenario steps defined both via the Ignite Operations DSL and Ignite Lambda DSL.
 * - DSL for SQL queries.
 * - Ignite gatling protocol configured via pre-started Ignite client node.
 * - Generate load with rising and decreasing RPS (0 -> 100 -> 0).
 *
 * Before run simulation start the Ignite server node manually on the localhost.
 */
class LambdaPutBinarySelect extends Simulation {
    // Start ignite client node to be used to generate the load.
    private val ignite = Ignition.start(new IgniteConfiguration().setClientMode(true))

    // Create test table before the simulation start.
    ignite.asInstanceOf[IgniteEx].context().query().querySqlFields(new SqlFieldsQuery(
        """
            CREATE TABLE IF NOT EXISTS City (
                Id INT(11),
                Name CHAR(35),
                PRIMARY KEY (ID)
            ) WITH "backups=1, cache_name=City, key_type=Integer, value_type=City";
        """), false)

    // Ignite put via binary object operation as Lambda.
    private val putOperation = { (ignite: Ignite, session: Session) =>
        val cache = ignite.cache("City").withKeepBinary[Int, BinaryObject]()

        val id = session("Id").as[Int]

        val value = ignite.binary().builder("City")
            .setField("Id", id)
            .setField("Name", ThreadLocalRandom.current().alphanumeric.take(20).mkString)
            .build()

        cache.put(id, value)
    }

    // Ignite SQL operation defined via the DSL.
    private val selectOperation =
        sql("City", "SELECT * FROM City WHERE Id=?")
            .args("#{Id}")
            .check(resultSet.count.is(1))

    private val feeder = Iterator.continually(Map(
        "Id" -> ThreadLocalRandom.current().nextInt()
    ))

    // Scenario calling both put and SQL select.
    private val scn = scenario("PutBinarySelect")
        .feed(feeder)
        .ignite (
            putOperation as "put",
            selectOperation as "select"
        )

    // Create protocol using the pre-started ignite client node.
    val protocol: IgniteProtocol = igniteProtocol.ignite(ignite)

    after {
        // Close ignite client after simulation end.
        ignite.close()
    }

    setUp(
        scn.inject(
            rampUsersPerSec(0) to 100 during 10.seconds,

            constantUsersPerSec(100) during 20.seconds,

            rampUsersPerSec(100) to 0 during 10.seconds
        )
    ).protocols(protocol)
        .assertions(global.failedRequests.count.is(0))
}
