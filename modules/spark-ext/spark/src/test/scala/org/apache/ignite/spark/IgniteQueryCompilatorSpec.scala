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

package org.apache.ignite.spark

import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, checkOptimizationResult, enclose}
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_TABLE}
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.lang.{Long => JLong}

@RunWith(classOf[JUnitRunner])
class IgniteQueryCompilatorSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Supported column and table names in lower case") {

        it("should successfully read table data via DataFrameReader") {
            val igniteDF = igniteSession.read
                .format(FORMAT_IGNITE)
                .option(OPTION_TABLE, "strings1")
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .load()

            assertResult(9)(igniteDF.count())
        }

        it("should successfully read table data from a single table via sql()") {
            val df = igniteSession.sql("SELECT UPPER(str) FROM strings1 WHERE id = 1")

            checkOptimizationResult(df, "SELECT UPPER(\"str\") AS \"upper(str)\" FROM \"strings1\" WHERE \"id\" = 1")

            val data = Tuple1("AAA")

            checkQueryData(df, data)
        }

        it("should successfully read table data from unioned tables via sql()") {
            val df = igniteSession.sql(
                "SELECT UPPER(str) FROM strings1 WHERE id = 1 " +
                    "UNION " +
                    "SELECT UPPER(str) FROM strings2 WHERE id = 7"
            )

            checkOptimizationResult(df, "SELECT \"upper(str)\" FROM (" +
                "SELECT UPPER(\"str\") AS \"upper(str)\" FROM \"strings1\" WHERE \"id\" = 1 " +
                "UNION " +
                "SELECT UPPER(\"str\") AS \"upper(str)\" FROM \"strings2\" WHERE \"id\" = 7" +
                ") table1")

            val data = (
                ("222"),
                ("AAA")
            )

            checkQueryData(df, data)
        }

        it("should successfully read table data from joined tables via sql()") {
            val df = igniteSession.sql("SELECT UPPER(s1.str) FROM strings1 s1 JOIN strings2 s2 ON s1.id = s2.id " +
                "WHERE s1.id = 1")

            checkOptimizationResult(df, "SELECT UPPER(\"strings1\".\"str\") AS \"upper(str)\" " +
                "FROM \"strings1\" JOIN \"strings2\" ON \"strings1\".\"id\" = \"strings2\".\"id\" " +
                "WHERE \"strings1\".\"id\" = 1 AND \"strings2\".\"id\" = 1")

            val data = Tuple1("AAA")

            checkQueryData(df, data)
        }

    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createStringTable(client, DEFAULT_CACHE, "strings1")
        createStringTable(client, DEFAULT_CACHE, "strings2")

        val configProvider = enclose(null)(x ⇒ () ⇒ {
            val cfg = IgnitionEx.loadConfiguration(TEST_CONFIG_FILE).get1()

            cfg.setClientMode(true)

            cfg.setIgniteInstanceName("client-2")

            cfg
        })

        igniteSession = IgniteSparkSession.builder()
            .config(spark.sparkContext.getConf)
            .config("spark.sql.caseSensitive", "true")
            .igniteConfigProvider(configProvider)
            .getOrCreate()
    }

    def createStringTable(client: Ignite, cacheName: String, tableName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            s"""
               | CREATE TABLE "$tableName" (
               |    "id" LONG,
               |    "str" VARCHAR,
               |    PRIMARY KEY ("id")) WITH "backups=1"
          """.stripMargin)).getAll

        val qry = new SqlFieldsQuery(s"""INSERT INTO \"$tableName\" (\"id\", \"str\") values (?, ?)""")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "aaa")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "AAA")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "AAA   ")).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], "   AAA")).getAll
        cache.query(qry.setArgs(5L.asInstanceOf[JLong], "   AAA   ")).getAll
        cache.query(qry.setArgs(6L.asInstanceOf[JLong], "ABCDEF")).getAll
        cache.query(qry.setArgs(7L.asInstanceOf[JLong], "222")).getAll
        cache.query(qry.setArgs(8L.asInstanceOf[JLong], null)).getAll
        cache.query(qry.setArgs(9L.asInstanceOf[JLong], "BAAAB")).getAll
    }
}
