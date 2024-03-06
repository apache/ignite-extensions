/*
 * Copyright 2023 JSC SberTech
 */

package org.apache.ignite.gatling.testsuites

import org.apache.ignite.gatling.BinaryTest
import org.apache.ignite.gatling.CreateCacheTest
import org.apache.ignite.gatling.InvokeAllTest
import org.apache.ignite.gatling.InvokeTest
import org.apache.ignite.gatling.LambdaTest
import org.apache.ignite.gatling.LockTest
import org.apache.ignite.gatling.ProtocolTest
import org.apache.ignite.gatling.PutAllGetAllTest
import org.apache.ignite.gatling.PutGetTest
import org.apache.ignite.gatling.SqlTest
import org.apache.ignite.gatling.TransactionInvalidAsyncOpsTest
import org.apache.ignite.gatling.TransactionInvalidParamsTest
import org.apache.ignite.gatling.TransactionTest
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
