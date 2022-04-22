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

import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/** Represents Ignite node facade operations. */
public interface IgniteFacade {
    /**
     * Gets existing cache with the given name or creates new one.
     *
     * @param <K> Key.
     * @param <V> Value.
     * @param name Cache name.
     * @return Cache facade that provides access to cache with given name.
     */
    public <K, V> IgniteCacheFacade<K, V> getOrCreateCache(String name);

    /**
     * Gets cache with the given name.
     *
     * @param <K> Key.
     * @param <V> Value.
     * @param name Cache name.
     * @return Cache facade that provides access to cache with specified name or {@code null} if it doesn't exist.
     */
    public <K, V> IgniteCacheFacade<K, V> cache(String name);

    /**
     * Gets the collection of names of currently available caches.
     *
     * @return Collection of names of currently available caches or an empty collection if no caches are available.
     */
    public Collection<String> cacheNames();

    /**
     * Gets an instance of {@link IgniteBinary} interface.
     *
     * @return Instance of {@link IgniteBinary} interface.
     */
    public IgniteBinary binary();

    /**
     * Destroys a cache with the given name and cleans data that was written to the cache. The call will
     * deallocate all resources associated with the given cache on all nodes in the cluster. There is no way
     * to undo the action and recover destroyed data.
     * If a cache with the specified name does not exist in the grid, the operation has no effect.
     *
     * @param cacheName Cache name to destroy.
     * @throws CacheException If error occurs.
     */
    void destroyCache(String cacheName);

    /**
     * @param connObj Object that will be used to obtain underlying Ignite client instance to access the Ignite cluster.
     * @return Ignite facade instance.
     */
    public static IgniteFacade of(Object connObj) {
        if (connObj instanceof Ignite)
            return new IgniteNodeFacade((Ignite)connObj);
        else if (connObj instanceof IgniteConfiguration) {
            try {
                return new IgniteNodeFacade(Ignition.ignite(((IgniteConfiguration)connObj).getIgniteInstanceName()));
            }
            catch (Exception ignored) {
                // No-op.
            }

            return new ClosableIgniteNodeFacade(Ignition.start((IgniteConfiguration)connObj));
        }
        else if (connObj instanceof String)
            return new ClosableIgniteNodeFacade(Ignition.start((String)connObj));
        else if (connObj instanceof IgniteClient)
            return new IgniteClientFacade((IgniteClient)connObj);
        else if (connObj instanceof ClientConfiguration)
            return new ClosableIgniteClientFacade(Ignition.startClient((ClientConfiguration)connObj));

        throw new IllegalArgumentException(
            "Object of type " + connObj.getClass().getName() + " can not be used to connect to the Ignite cluster.");
    }
}
