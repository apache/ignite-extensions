/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.api

import java.util.concurrent.locks.Lock

import javax.cache.processor.EntryProcessorResult

import scala.collection.SortedMap
import scala.collection.SortedSet

import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.cache.query.SqlFieldsQuery

/**
 * Wrapper around the Ignite Cache API abstracting access either via the Ignite node (thick) API or vai the
 * Ignite Client (thin) API.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 */
trait CacheApi[K, V] {
    /**
     * Puts entry to cache.
     *
     * @param key Key.
     * @param value Value.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def put(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously puts entry to cache.
     *
     * @param key Key.
     * @param value Value.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def putAsync(key: K, value: V)(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Puts batch of entries to cache.
     *
     * @param map Key / value pairs map.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def putAll(map: SortedMap[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously puts batch of entries to cache.
     *
     * @param map Key / value pairs map.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def putAllAsync(map: SortedMap[K, V])(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Gets an entry from cache by key.
     *
     * @param key Key.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def get(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously gets an entry from cache by key.
     *
     * @param key Key.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def getAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Gets a collection of entries from the cache.
     *
     * @param keys Collection of keys.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def getAll(keys: SortedSet[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously gets a collection of entries from the cache.
     *
     * @param keys Collection of keys.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def getAllAsync(keys: SortedSet[K])(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Removes the entry for a key only if currently mapped to some.
     *
     * @param key Key.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def getAndRemove(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously removes the entry for a key only if currently mapped to some.
     *
     * @param key Key.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def getAndRemoveAsync(key: K)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Puts value with the specified key in this cache, returning an existing value
     * if one existed.
     *
     * @param key Key.
     * @param value Value.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def getAndPut(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously puts value with the specified key in this cache, returning an existing
     * value if one existed.
     *
     * @param key Key.
     * @param value Value.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def getAndPutAsync(key: K, value: V)(s: Map[K, V] => Unit, f: Throwable => Unit): Unit

    /**
     * Removes the entry for a key.
     *
     * @param key Key.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def remove(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously removes the entry for a key.
     *
     * @param key Key.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def removeAsync(key: K)(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Removes a collection of entries from the cache.
     *
     * @param keys Set of keys.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def removeAll(keys: SortedSet[K])(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Asynchronously removes a collection of entries from the cache.
     *
     * @param keys Set of keys.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def removeAllAsync(keys: SortedSet[K])(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Invokes an CacheEntryProcessor against the entry specified by the provided key.
     *
     * @tparam T Type of the return value.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param arguments Additional arguments to pass to the entry processor.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def invoke[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
        s: Map[K, T] => Unit,
        f: Throwable => Unit
    ): Unit

    /**
     * Asynchronously invokes an CacheEntryProcessor against the entry specified by the provided key.
     *
     * @tparam T Type of the return value.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param arguments Additional arguments to pass to the entry processor.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def invokeAsync[T](key: K, entryProcessor: CacheEntryProcessor[K, V, T], arguments: Any*)(
        s: Map[K, T] => Unit,
        f: Throwable => Unit
    ): Unit

    /**
     * Invokes each EntryProcessor from map's values against the correspondent
     * entry specified by map's key set.
     *
     * @tparam T Type of the return value.
     * @param map Map containing keys and entry processors to be applied to values.
     * @param arguments Additional arguments to pass to the entry processors.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def invokeAll[T](map: SortedMap[K, CacheEntryProcessor[K, V, T]], arguments: Any*)(
        s: Map[K, EntryProcessorResult[T]] => Unit,
        f: Throwable => Unit
    ): Unit

    /**
     * Asynchronously invokes each EntryProcessor from map's values against the correspondent
     * entry specified by map's key set.
     *
     * @tparam T Type of the return value.
     * @param map Map containing keys and entry processors to be applied to values.
     * @param arguments Additional arguments to pass to the entry processors.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def invokeAllAsync[T](map: SortedMap[K, CacheEntryProcessor[K, V, T]], arguments: Any*)(
        s: Map[K, EntryProcessorResult[T]] => Unit,
        f: Throwable => Unit
    ): Unit

    /**
     * Acquires lock associated with a passed key.
     *
     * @param key Key.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def lock(key: K)(s: Lock => Unit, f: Throwable => Unit): Unit

    /**
     * Releases the lock.
     *
     * @param lock Lock.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def unlock(lock: Lock)(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Executes the Ignite SqlFieldsQuery
     *
     * @param query Query object.
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def sql(query: SqlFieldsQuery)(s: List[List[Any]] => Unit, f: Throwable => Unit): Unit

    /**
     * Returns cache that will operate with binary objects.
     *
     * @return Instance of Cache API.
     */
    def withKeepBinary(): CacheApi[K, V]
}
