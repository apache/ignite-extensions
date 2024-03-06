/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.api.node

import java.util.concurrent.locks.Lock

import javax.cache.processor.EntryProcessorResult

import scala.collection.SortedMap
import scala.collection.SortedSet
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.ignite.gatling.api.CacheApi
import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery

/**
 * Implementation of CacheApi working via the Ignite Node (thick) API.
 *
 * @param wrapped Instance of Cache API.
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 */
case class CacheNodeApi[K, V](wrapped: IgniteCache[K, V]) extends CacheApi[K, V] with StrictLogging {
    override def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.put(key, value))
            .map(_ => ())
            .fold(f, s)
    }

    override def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .putAsync(key, value)
            .listen(future =>
                Try(future.get())
                    .map(_ => ())
                    .fold(f, s)
            )
    }

    override def putAll(map: SortedMap[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.putAll(map.asJava))
            .map(_ => ())
            .fold(f, s)
    }

    override def putAllAsync(map: SortedMap[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .putAllAsync(map.asJava)
            .listen(future =>
                Try(future.get())
                    .map(_ => ())
                    .fold(f, s)
            )
    }

    override def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.get(key))
            .map(value => Map((key, value)))
            .fold(f, s)
    }

    override def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .getAsync(key)
            .listen(future =>
                Try(future.get())
                    .map(value => Map((key, value)))
                    .fold(f, s)
            )
    }

    override def getAndRemove(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.getAndRemove(key))
            .map(value => Map((key, value)))
            .fold(f, s)
    }

    override def getAndRemoveAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .getAndRemoveAsync(key)
            .listen(future =>
                Try(future.get())
                    .map(value => Map((key, value)))
                    .fold(f, s)
            )
    }

    override def getAndPut(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.getAndPut(key, value))
            .map(value => Map((key, value)))
            .fold(f, s)
    }

    override def getAndPutAsync(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .getAndPutAsync(key, value)
            .listen(future =>
                Try(future.get())
                    .map(value => Map((key, value)))
                    .fold(f, s)
            )
    }

    override def getAll(keys: SortedSet[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.getAll(keys.asJava))
            .map(_.asScala.toMap)
            .fold(f, s)
    }

    override def getAllAsync(keys: SortedSet[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .getAllAsync(keys.asJava)
            .listen(future =>
                Try(future.get())
                    .map(_.asScala.toMap)
                    .fold(f, s)
            )
    }

    override def remove(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.remove(key))
            .map(_ => ())
            .fold(f, s)
    }

    override def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .removeAsync(key)
            .listen(future =>
                Try(future.get())
                    .map(_ => ())
                    .fold(f, s)
            )
    }

    override def removeAll(keys: SortedSet[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.removeAll(keys.asJava))
            .map(_ => ())
            .fold(f, s)
    }

    override def removeAllAsync(keys: SortedSet[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .removeAllAsync(keys.asJava)
            .listen(future =>
                Try(future.get())
                    .map(_ => ())
                    .fold(f, s)
            )
    }

    override def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
        s: Map[K, T] => Unit,
        f: Throwable => Unit
    ): Unit = {
        Try(wrapped.invoke[T](key, entryProcessor, arguments: _*))
            .map(value => Map((key, value)))
            .fold(f, s)
    }

    override def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
        s: Map[K, T] => Unit,
        f: Throwable => Unit
    ): Unit = {
        wrapped
            .invokeAsync(key, entryProcessor, arguments: _*)
            .listen(future =>
                Try(future.get())
                    .map(value => Map((key, value)))
                    .fold(f, s)
            )
    }

    override def invokeAll[T](
        map: SortedMap[K, CacheEntryProcessor[K, V, T]],
        arguments: Any*
    )(s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.invokeAll[T](map.asJava, arguments: _*))
            .map(_.asScala.toMap)
            .fold(f, s)
    }

    override def invokeAllAsync[T](
        map: SortedMap[K, CacheEntryProcessor[K, V, T]],
        arguments: Any*
    )(s: Map[K, EntryProcessorResult[T]] => Unit, f: Throwable => Unit): Unit = {
        wrapped
            .invokeAllAsync(map.asJava, arguments: _*)
            .listen(future =>
                Try(future.get())
                    .map(_.asScala.toMap)
                    .fold(f, s)
            )
    }

    override def lock(key: K)(s: Lock => Unit, f: Throwable => Unit): Unit = {
        Try {
            val lock = wrapped.lock(key)
            lock.lock()
            lock
        }.fold(f, s)
    }

    override def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(lock.unlock())
            .fold(f, s)
    }

    override def sql(query: SqlFieldsQuery)(s: List[List[Any]] => Unit, f: Throwable => Unit): Unit = {
        Try(
            wrapped
                .query(query)
                .getAll
                .asScala
                .toList
                .map(_.asScala.toList)
        ).fold(f, s)
    }

    override def withKeepBinary(): CacheApi[K, V] = copy(wrapped = wrapped.withKeepBinary())
}
