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

package org.apache.ignite.springdata20.repository.support.client;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.springdata20.repository.support.IgniteCacheProxy;
import org.apache.ignite.springdata20.repository.support.IgniteProxy;

/** Implementation of {@link IgniteProxy} that provides access to Ignite cluster through {@link IgniteClient} instance. */
public class IgniteClientProxy implements IgniteProxy {
    /** {@link IgniteClient} instance to which operations are delegated.  */
    private final IgniteClient cli;

    /** */
    public IgniteClientProxy(IgniteClient cli) {
        this.cli = cli;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheProxy<K, V> getOrCreateCache(String name) {
        return new IgniteCacheClientProxy<>(cli.getOrCreateCache(name));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheProxy<K, V> cache(String name) {
        return new IgniteCacheClientProxy<>(cli.cache(name));
    }
}
