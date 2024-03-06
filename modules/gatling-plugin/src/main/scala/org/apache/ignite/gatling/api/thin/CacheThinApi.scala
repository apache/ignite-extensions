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

import java.util.concurrent.locks.Lock

import javax.cache.processor.EntryProcessorResult

import scala.collection.SortedMap
import scala.collection.SortedSet
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.client.ClientCache
import org.apache.ignite.gatling.api.CacheApi

/**
 * Implementation of CacheApi working via the Ignite (thin) Client API.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param wrapped Instance of Ignite Client Cache API.
 */
case class CacheThinApi[K, V](wrapped: ClientCache[K, V]) extends CacheApi[K, V] with CompletionSupport with StrictLogging {

    override def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.put(key, value))
            .map(_ => ())
            .fold(f, s)
    }

    override def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.putAsync(key, value).asScala.map(_ => ())
        }(s, f)
    }

    override def putAll(map: SortedMap[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.putAll(map.asJava))
            .map(_ => ())
            .fold(f, s)
    }

    override def putAllAsync(map: SortedMap[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.putAllAsync(map.asJava).asScala.map(_ => ())
        }(s, f)
    }

    override def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.get(key))
            .map(v => Map((key, v)))
            .fold(f, s)
    }

    override def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.getAsync(key).asScala.map(v => Map((key, v)))
        }(s, f)
    }

    override def getAndRemove(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.getAndRemove(key))
            .map(v => Map((key, v)))
            .fold(f, s)
    }

    override def getAndRemoveAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.getAndRemoveAsync(key).asScala.map(v => Map((key, v)))
        }(s, f)
    }

    override def getAndPut(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.getAndPut(key, value))
            .map(v => Map((key, v)))
            .fold(f, s)
    }

    override def getAndPutAsync(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.getAndPutAsync(key, value).asScala.map(v => Map((key, v)))
        }(s, f)
    }

    override def getAll(keys: SortedSet[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.getAll(keys.asJava))
            .map(_.asScala.toMap)
            .fold(f, s)
    }

    override def getAllAsync(keys: SortedSet[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.getAllAsync(keys.asJava).asScala.map(v => v.asScala.toMap)
        }(s, f)
    }

    override def remove(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.remove(key))
            .map(_ => ())
            .fold(f, s)
    }

    override def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.removeAsync(key).asScala.map(_ => ())
        }(s, f)
    }

    override def removeAll(keys: SortedSet[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        Try(wrapped.removeAll(keys.asJava))
            .map(_ => ())
            .fold(f, s)
    }

    override def removeAllAsync(keys: SortedSet[K])(s: Unit => Unit, f: Throwable => Unit): Unit = {
        withCompletion {
            wrapped.removeAllAsync(keys.asJava).asScala.map(_ => ())
        }(s, f)
    }

    override def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
        s: Map[K, T] => Unit,
        f: Throwable => Unit
    ): Unit =
        f(new NotImplementedError("invoke is not supported in thin client API"))

    override def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
        s: Map[K, T] => Unit,
        f: Throwable => Unit
    ): Unit =
        f(new NotImplementedError("invokeAsync is not supported in thin client API"))

    override def invokeAll[T](map: SortedMap[K, CacheEntryProcessor[K, V, T]], arguments: Any*)(
        s: Map[K, EntryProcessorResult[T]] => Unit,
        f: Throwable => Unit
    ): Unit =
        f(new NotImplementedError("invokeAll is not supported in thin client API"))

    override def invokeAllAsync[T](map: SortedMap[K, CacheEntryProcessor[K, V, T]], arguments: Any*)(
        s: Map[K, EntryProcessorResult[T]] => Unit,
        f: Throwable => Unit
    ): Unit =
        f(new NotImplementedError("invokeAllAsync is not supported in thin client API"))

    override def lock(key: K)(s: Lock => Unit, f: Throwable => Unit): Unit =
        f(new NotImplementedError("lock is not supported in thin client API"))

    override def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit): Unit =
        f(new NotImplementedError("unlock is not supported in thin client API"))

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
