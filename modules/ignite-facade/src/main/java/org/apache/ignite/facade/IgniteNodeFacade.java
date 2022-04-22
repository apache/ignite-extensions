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
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;

/** Implementation of {@link IgniteFacade} that provides access to Ignite cluster through {@link Ignite} instance. */
public class IgniteNodeFacade implements IgniteFacade {
    /** {@link Ignite} instance to which operations are delegated. */
    protected final Ignite ignite;

    /**
     * @param ignite Ignite instance.
     */
    public IgniteNodeFacade(Ignite ignite) {
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheFacade<K, V> getOrCreateCache(String name) {
        return new IgniteNodeCacheFacade<>(ignite.getOrCreateCache(name));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheFacade<K, V> cache(String name) {
        IgniteCache<K, V> cache = ignite.cache(name);

        return cache == null ? null : new IgniteNodeCacheFacade<>(cache);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        return ignite.cacheNames();
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return ignite.binary();
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        ignite.destroyCache(cacheName);
    }

    /** @return {@link Ignite} instance to which operations are delegated. */
    public Ignite delegate() {
        return ignite;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        return Objects.equals(ignite, ((IgniteNodeFacade)other).ignite);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return ignite.hashCode();
    }
}
