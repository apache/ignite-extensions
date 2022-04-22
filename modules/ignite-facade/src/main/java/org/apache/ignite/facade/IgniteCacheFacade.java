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

package org.apache.ignite.facade;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;

/** Represents Ignite cache facade operations. */
public interface IgniteCacheFacade<K, V> extends Iterable<Cache.Entry<K, V>> {
    /**
     * Gets an entry from the cache.
     *
     * @param key the key whose associated value is to be returned
     * @return the element, or null, if it does not exist.
     */
    public V get(K key);

    /**
     * Associates the specified value with the specified key in the cache.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key.
     */
    public void put(K key, V val);

    /**
     * Gets the number of all entries cached across all nodes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return The number of all entries cached across all nodes.
     */
    public int size(CachePeekMode... peekModes);

    /**
     * Gets a collection of entries from the Ignite cache, returning them as
     * {@link Map} of the values associated with the set of keys requested.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return A map of entries that were found for the given keys.
     */
    public Map<K, V> getAll(Set<? extends K> keys);

    /**
     * Copies all of the entries from the specified map to the Ignite cache.
     *
     * @param map Mappings to be stored in this cache.
     */
    public void putAll(Map<? extends K, ? extends V> map);

    /**
     * Removes the mapping for a key from this cache if it is present.
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @return <tt>false</tt> if there was no matching key.
     */
    public boolean remove(K key);

    /**
     * Removes entries for the specified keys.
     *
     * @param keys The keys to remove.
     */
    public void removeAll(Set<? extends K> keys);

    /** Clears the contents of the cache. */
    public void clear();

    /**
     * Returns cache with the specified expired policy set. This policy will be used for each operation invoked on
     * the returned cache.
     *
     * @param expirePlc Expire policy.
     * @return Cache instance with the specified expiry policy set.
     */
    public IgniteCacheFacade<K, V> withExpiryPolicy(ExpiryPolicy expirePlc);

    /**
     * Execute SQL query and get cursor to iterate over results.
     *
     * @param <R> Type of query data.
     * @param qry SQL query.
     * @return Query result cursor.
     */
    public <R> QueryCursor<R> query(Query<R> qry);

    /**
     * Determines if the {@link ClientCache} contains an entry for the specified key.
     *
     * @param key key whose presence in this cache is to be tested.
     * @return <tt>true</tt> if this map contains a mapping for the specified key.
     */
    public boolean containsKey(K key);

    /** @return The name of the cache. */
    public String getName();

    /** @return Cache with read-through write-through behavior disabled. */
    public IgniteCacheFacade<K, V> withSkipStore();

    /**
     * Atomically associates the specified key with the given value if it is not already associated with a value.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Value that is already associated with the specified key, or {@code null} if no value was associated
     * with the specified key and a value was set.
     */
    public V getAndPutIfAbsent(K key, V val);

    /** Removes all of the mappings from this cache. */
    public void removeAll();

    /**
     * Deregisters a listener, using the {@link CacheEntryListenerConfiguration} that was used to register it.
     *
     * @param cacheEntryListenerConfiguration the factory and related configuration that was used to create the
     *         listener.
     */
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

    /**
     * Execute SQL query and get cursor to iterate over results.
     *
     * @param qry SQL query.
     * @return Cursor.
     */
    public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry);

    /**
     * Atomically replaces the entry for a key only if currently mapped to some
     * value.
     *
     * @param key The key with which the specified value is associated.
     * @param val The value to be associated with the specified key.
     * @return <tt>true</tt> if the value was replaced.
     */
    public boolean replace(K key, V val);

    /**
     * Atomically replaces the entry for a key only if currently mapped to a given value.
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Value expected to be associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return <tt>true</tt> if the value was replaced
     */
    public boolean replace(K key, V oldVal, V newVal);

    /**
     * Determines if the cache contains entries for the specified keys.
     *
     * @param keys Keys whose presence in this cache is to be tested.
     * @return {@code True} if this cache contains a mapping for the specified keys.
     */
    public boolean containsKeys(Set<? extends K> keys);

    /**
     * Clears entries with specified keys from the cache.
     * In contrast to {@link #removeAll(Set)}, this method does not notify event listeners and cache writers.
     *
     * @param keys Cache entry keys to clear.
     */
    public void clearAll(Set<? extends K> keys);

    /**
     * Registers a {@link CacheEntryListener}. The supplied {@link CacheEntryListenerConfiguration} is used to
     * instantiate a listener and apply it to those events specified in the configuration.
     * <p>
     *
     * @param cacheEntryListenerConfiguration a factory and related configuration for creating the listener.
     */
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

    /**
     * Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return The previous value associated with the specified key, or
     * <tt>null</tt> if there was no mapping for the key.
     */
    public V getAndReplace(K key, V val);

    /**
     * Associates the specified value with the specified key in this cache, returning an existing value if one existed.
     * The previous value is returned, or null if there was no value associated
     * with the key previously.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return The value associated with the key at the start of the operation or
     * null if none was associated.
     */
    public V getAndPut(K key, V val);

    /**
     * Atomically associates the specified key with the given value if it is not already associated with a value.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return <tt>true</tt> if a value was set.
     */
    public boolean putIfAbsent(K key, V val);

    /**
     * Atomically removes the entry for a key only if currently mapped to some value.
     *
     * @param key Key with which the specified value is associated.
     * @return The value if one existed or null if no mapping existed for this key.
     */
    public V getAndRemove(K key);

    /** @return Cache instance to which the current facade delegates operations. */
    public Object delegate();
}
