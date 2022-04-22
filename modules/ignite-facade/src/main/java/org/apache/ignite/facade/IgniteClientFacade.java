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
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.client.IgniteClient;

/** Implementation of {@link IgniteFacade} that provides access to Ignite cluster through {@link IgniteClient} instance. */
public class IgniteClientFacade implements IgniteFacade {
    /** {@link IgniteClient} instance to which operations are delegated.  */
    protected final IgniteClient cli;

    /**
     * @param cli Ignite client.
     */
    public IgniteClientFacade(IgniteClient cli) {
        this.cli = cli;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheFacade<K, V> getOrCreateCache(String name) {
        return new IgniteClientCacheFacade<>(cli.getOrCreateCache(name));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheFacade<K, V> cache(String name) {
        return new IgniteClientCacheFacade<>(cli.cache(name));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        return cli.cacheNames();
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return cli.binary();
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        cli.destroyCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        return Objects.equals(cli, ((IgniteClientFacade)other).cli);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return cli.hashCode();
    }
}
