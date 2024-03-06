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
package org.apache.ignite.gatling.api.node

import scala.util.Try

import io.gatling.commons.validation.{Failure => ValidationFailure}
import io.gatling.commons.validation.{Success => ValidationSuccess}
import io.gatling.commons.validation.Validation
import org.apache.ignite.Ignite
import org.apache.ignite.binary.BinaryObjectBuilder
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation
import scalaz.Memo

/**
 * Implementation of IgniteApi working via the Ignite node (thick) API.
 *
 * @param wrapped Instance of Ignite API.
 */
case class IgniteNodeApi(wrapped: Ignite) extends IgniteApi {

    override def cache[K, V]: (String => Validation[CacheApi[K, V]]) = Memo.immutableHashMapMemo[String, Validation[CacheApi[K, V]]] { name =>
        Try(wrapped.cache[K, V](name))
            .map(CacheNodeApi[K, V])
            .fold(ex => ValidationFailure(ex.getMessage), ValidationSuccess(_))
    }

    override def getOrCreateCacheByClientConfiguration[K, V](
        cfg: ClientCacheConfiguration
    )(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
        f(new NotImplementedError("Thin client cache configuration was used to create cache via node API"))

    override def getOrCreateCacheByConfiguration[K, V](
        cfg: CacheConfiguration[K, V]
    )(s: CacheApi[K, V] => Unit, f: Throwable => Unit): Unit =
        Try(wrapped.getOrCreateCache[K, V](cfg))
            .map(CacheNodeApi[K, V])
            .fold(f, s)

    override def getOrCreateCacheBySimpleConfig[K, V](name: String, cfg: SimpleCacheConfiguration)(
        s: CacheApi[K, V] => Unit,
        f: Throwable => Unit
    ): Unit =
        getOrCreateCacheByConfiguration(
            new CacheConfiguration[K, V]()
                .setName(name)
                .setCacheMode(cfg.cacheMode)
                .setAtomicityMode(cfg.atomicity)
                .setBackups(cfg.backups)
        )(s, f)

    override def close()(s: Unit => Unit, f: Throwable => Unit): Unit =
        Try(wrapped.close()).fold(f, s)

    override def txStart()(s: TransactionApi => Unit, f: Throwable => Unit): Unit =
        Try(wrapped.transactions().txStart())
            .map(TransactionNodeApi)
            .fold(f, s)

    override def txStart(
        concurrency: TransactionConcurrency,
        isolation: TransactionIsolation
    )(s: TransactionApi => Unit, f: Throwable => Unit): Unit =
        Try(wrapped.transactions().txStart(concurrency, isolation))
            .map(TransactionNodeApi)
            .fold(f, s)

    override def txStart(concurrency: TransactionConcurrency, isolation: TransactionIsolation, timeout: Long, size: Int)(
        s: TransactionApi => Unit,
        f: Throwable => Unit
    ): Unit =
        Try(wrapped.transactions().txStart(concurrency, isolation, timeout, size))
            .map(TransactionNodeApi)
            .fold(f, s)

    override def binaryObjectBuilder: String => BinaryObjectBuilder = typeName => wrapped.binary().builder(typeName)

    override def wrapped[I]: I = wrapped.asInstanceOf[I]
}
