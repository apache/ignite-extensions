/*
 * Copyright 2023 JSC SberTech
 */

package com.sbt.ignite.gatling.testsuites

import com.sbt.ignite.gatling.BinaryTest
import com.sbt.ignite.gatling.CreateCacheTest
import com.sbt.ignite.gatling.InvokeAllTest
import com.sbt.ignite.gatling.InvokeTest
import com.sbt.ignite.gatling.LambdaTest
import com.sbt.ignite.gatling.LockTest
import com.sbt.ignite.gatling.ProtocolTest
import com.sbt.ignite.gatling.PutAllGetAllTest
import com.sbt.ignite.gatling.PutGetTest
import com.sbt.ignite.gatling.SqlTest
import com.sbt.ignite.gatling.TransactionInvalidAsyncOpsTest
import com.sbt.ignite.gatling.TransactionInvalidParamsTest
import com.sbt.ignite.gatling.TransactionTest
import org.junit.runner.RunWith
import org.junit.runners.Suite

/**
 * Ignite Gatling plugin tests.
 */
@RunWith(classOf[Suite])
@Suite.SuiteClasses(
    Array(
        classOf[BinaryTest],
        classOf[CreateCacheTest],
        classOf[InvokeAllTest],
        classOf[InvokeTest],
        classOf[LambdaTest],
        classOf[LockTest],
        classOf[ProtocolTest],
        classOf[PutAllGetAllTest],
        classOf[PutGetTest],
        classOf[SqlTest],
        classOf[TransactionInvalidAsyncOpsTest],
        classOf[TransactionInvalidParamsTest],
        classOf[TransactionTest]
    )
)
class IgniteGatlingPluginTestSuite
