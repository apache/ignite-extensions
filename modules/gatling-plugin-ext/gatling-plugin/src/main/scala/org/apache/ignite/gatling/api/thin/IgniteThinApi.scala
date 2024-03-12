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
package org.apache.ignite.gatling.api.thin

import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

import io.gatling.commons.validation.{Failure => ValidationFailure}
import io.gatling.commons.validation.{Success => ValidationSuccess}
import io.gatling.commons.validation.Validation
import org.apache.ignite.binary.BinaryObjectBuilder
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.gatling.protocol.IgniteClientPool
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation
import scalaz.Memo

/**
 * Implementation of IgniteApi working via the Ignite (thin) Client API.
 *
 * @param clientPool Pool of the IgniteClient instances.
 */
case class IgniteThinApi(clientPool: IgniteClientPool) extends IgniteApi with CompletionSupport {

    override def cache[K, V]: String => Validation[CacheApi[K, V]] = Memo.immutableHashMapMemo[String, Validation[CacheApi[K, V]]] { name =>
        Try(clientPool().cache[K, V](name))
            .map(CacheThinApi[K, V])
            .fold(ex => ValidationFailure(ex.getMessage), ValidationSuccess(_))
    }

    override def getOrCreateCacheByClientConfiguration[K, V](
        cfg: ClientCacheConfiguration
    )(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
        withCompletion(clientPool().getOrCreateCacheAsync[K, V](cfg).asScala.map(CacheThinApi[K, V]))(s, f)

    override def getOrCreateCacheByConfiguration[K, V](
        cfg: CacheConfiguration[K, V]
    )(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
        f(new NotImplementedError("Node client cache configuration was used to create cache via thin client API"))

    override def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)(
        s: CacheApi[K, V] => Unit,
        f: Throwable => Unit
    ): Unit =
        getOrCreateCacheByClientConfiguration(
            new ClientCacheConfiguration()
                .setName(name)
                .setCacheMode(cfg.cacheMode)
                .setAtomicityMode(cfg.atomicity)
                .setBackups(cfg.backups)
        )(s, f)

    override def close()(s: Unit => Unit, f: Throwable => Unit): Unit =
        Try(clientPool().close()).fold(f, s)

    override def txStart()(s: TransactionApi => Unit, f: Throwable => Unit): Unit =
        Try(clientPool().transactions().txStart())
            .map(TransactionThinApi)
            .fold(f, s)

    override def txStart(
        concurrency: TransactionConcurrency,
        isolation: TransactionIsolation
    )(s: TransactionApi => Unit, f: Throwable => Unit): Unit =
        Try(clientPool().transactions().txStart(concurrency, isolation))
            .map(TransactionThinApi)
            .fold(f, s)

    override def txStart(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, size: Int)(
        s: TransactionApi => Unit,
        f: Throwable => Unit
    ): Unit =
        Try(clientPool().transactions().txStart(concurrency, isolation, timeout))
            .map(TransactionThinApi)
            .fold(f, s)

    override def binaryObjectBuilder: String => BinaryObjectBuilder = typeName => clientPool().binary().builder(typeName)

    override def wrapped[I]: I = clientPool().asInstanceOf[I]
}
