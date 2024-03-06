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

import scala.io.Source

import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.Predef._
import org.junit.Test

/**
 * Tests SQL queries.
 */
class SqlTest extends AbstractGatlingTest {
    /** Class name of simulation */
    val simulation: String = classOf[SqlSimulation].getName

    /** Runs simulation with thin client. */
    @Test
    def sqlThinClient(): Unit = runWith(ThinClient)(classOf[SqlSimulation].getName)

    /** Runs simulation with thick client. */
    @Test
    def sqlThickClient(): Unit = runWith(NodeApi)(classOf[SqlSimulation].getName)

    /** Runs DDL script simulation with thick client. */
    @Test
    def ddlScriptThickClient(): Unit = runWith(NodeApi)(classOf[SqlDdlSimulation].getName)

    /** Runs DDL script simulation with thin client. */
    @Test
    def ddlScriptThinClient(): Unit = runWith(ThinClient)(classOf[SqlDdlSimulation].getName)
}

/**
 * SQL simulation.
 */
class SqlSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val cache = "TEST-CACHE"

    private val keyExpression = (s: Session) => s("key").as[Int].success

    private val scn = scenario("Sql")
        .feed(feeder)
        .ignite(
            startIgniteApi
                as "Start client",
            getOrCreateCache(cache).backups(1)
                as "Create cache",
            sql(cache, "CREATE TABLE City (id int primary key, name varchar, region varchar)")
                as "Create table",
            sql(cache, "INSERT INTO City(id, name, region) VALUES(?, ?, ?)")
                .args("#{key}", "City 1", "Region")
                as "Insert",
            sql(cache, "INSERT INTO City(id, name, region) VALUES(?, ?, ?)")
                .args(s => s("key").as[Int] + 1, "City 2", "Region"),
            sql(cache, "SELECT * FROM City WHERE id = ?")
                .args(keyExpression)
                .check(
                    resultSet,
                    resultSet.count.is(1),
                    resultSet.count.gte(0),
                    resultSet.find,
                    resultSet.find.saveAs("firstRow"),
                    resultSet.find.transform(r => r(2)),
                    resultSet.find.transform(r => r(2)).is("Region").saveAs("Region"),
                    resultSet.validate((row: Row, _: Session) => row(2) == "RR").name("named check to fail"),
                    resultSet.findAll.validate((rows: Seq[Row], _: Session) => rows.head(2) == "Region")
                ) as "Select",
            sql(cache, "SELECT * FROM City WHERE Region = ? ORDER BY name")
                .args("Region")
                .partitions(List(1))
                .check(
                    resultSet,
                    resultSet.count.is(2),
                    resultSet.find(1),
                    resultSet.find(1).saveAs("secondRow"),
                    resultSet.find(1).transform(r => r(2)),
                    resultSet.find(1).transform(r => r(1)).is("City 2").saveAs("City"),
                    resultSet.validate((row: Row, _: Session) => row(2) == "RR").name("named check to fail"),
                    resultSet.findAll.validate((rows: Seq[Row], _: Session) => rows.head(1) == "City 1")
                )
        )
        .exec { session =>
            logger.info(session.toString)
            session
        }
        .exec(closeIgniteApi as "Close client")

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(2)
        )
}

/**
 * Execute DDL script simulation.
 */
class SqlDdlSimulation extends Simulation with IgniteSupport with StrictLogging {
    private val scn = scenario("Sql")
        .ignite(
            getOrCreateCache("some-cache"),
            foreach(Source.fromResource("ddl.sql").getLines().toList, "query")(
                sql("some-cache", "#{query}")
            )
        )

    setUp(scn.inject(atOnceUsers(1)))
        .protocols(protocol)
        .assertions(
            global.failedRequests.count.is(0)
        )
}
