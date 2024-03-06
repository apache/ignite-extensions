/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.api.thin

import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

import com.sbt.ignite.gatling.api.CacheApi
import com.sbt.ignite.gatling.api.IgniteApi
import com.sbt.ignite.gatling.api.TransactionApi
import com.sbt.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import com.sbt.ignite.gatling.protocol.IgniteClientPool
import io.gatling.commons.validation.{Failure => ValidationFailure}
import io.gatling.commons.validation.{Success => ValidationSuccess}
import io.gatling.commons.validation.Validation
import org.apache.ignite.binary.BinaryObjectBuilder
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
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
