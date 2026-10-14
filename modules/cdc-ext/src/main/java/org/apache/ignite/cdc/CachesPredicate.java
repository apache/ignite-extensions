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

package org.apache.ignite.cdc;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Predicate for filtering {@link CdcEvent}s inside {@link CdcConsumer#onEvents(Iterator)}. Filters out events for
 * following types of caches:
 * <ol>
 *   <li>Caches set in CDC configuration.</li>
 *   <li>Caches that are added dynamically by user's cache regexp templates.</li>
 * </ol>
 */
public class CachesPredicate implements Predicate<Integer> {
    /** Cache names. */
    private Collection<String> caches;

    /** Include regex template */
    private String includeRegex;

    /** Exclude regex template */
    private String excludeRegex;

    /** Include regex pattern for cache names. */
    private Pattern includePtrn;

    /** Exclude regex pattern for cache names. */
    private Pattern excludePtrn;

    /** Cache IDs. */
    private Set<Integer> cacheIds;

    /** Cache regex IDs. */
    private final Set<Integer> cacheRegexIds = new ConcurrentSkipListSet<>();

    /** Logger. */
    private IgniteLogger log;

    /** */
    public void init(IgniteLogger log, Iterator<CdcCacheEvent> cacheEvents) {
        this.log = log;

        if (includeRegex == null)
            A.notEmpty(caches, "caches");

        cacheIds = caches == null
            ? Collections.emptySet()
            : caches.stream()
                .mapToInt(CU::cacheId)
                .boxed()
                .collect(Collectors.toCollection(HashSet::new));

        try {
            includePtrn = includeRegex != null ? Pattern.compile(includeRegex) : null;
            excludePtrn = excludeRegex != null ? Pattern.compile(excludeRegex) : null;
        }
        catch (PatternSyntaxException e) {
            throw new IgniteException("Invalid cache regexp template", e);
        }

        cacheEvents.forEachRemaining(evt -> onCacheEvent(evt.configuration().getName()));
    }

    /**
     * Sets cache ids of caches participating in CDC.
     * @param caches Cache names.
     */
    public void setCaches(Collection<String> caches) {
        this.caches = caches;
    }

    /**
     * Sets include regex pattern for caches participating in CDC.
     *
     * @param includeRegex Include regex string.
     * @throws IgniteException If the template's syntax is invalid.
     */
    public void setIncludeCacheTemplate(String includeRegex) {
        this.includeRegex = includeRegex;
    }

    /**
     * Sets exclude regex pattern for caches participating in CDC.
     *
     * @param excludeRegex Exclude regex string.
     * @throws IgniteException If the template's syntax is invalid.
     */
    public void setExcludeCacheTemplate(String excludeRegex) {
        this.excludeRegex = excludeRegex;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Integer cacheId) {
        return cacheIds.contains(cacheId) || cacheRegexIds.contains(cacheId);
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if the cache is configured explicitly or matches the regex filters.
     */
    public boolean matches(String cacheName) {
        return cacheIds.contains(CU.cacheId(cacheName)) || matchesRegex(cacheName);
    }

    /**
     * Cache start event listener.
     *
     * @param cacheName Cache name.
     */
    public void onCacheEvent(String cacheName) {
        if (!cacheIds.contains(CU.cacheId(cacheName)) && matchesRegex(cacheName)) {
            boolean added = cacheRegexIds.add(CU.cacheId(cacheName));

            if (added && log.isInfoEnabled())
                log.info("Cache matched CDC regex filter [cacheName=" + cacheName + ']');
        }
    }

    /** */
    public void onCacheDestroy(int cacheId) {
        boolean removed = cacheRegexIds.remove(cacheId);

        if (removed && log.isInfoEnabled())
            log.info("Destroyed cache removed from CDC regex filter [cacheId=" + cacheId + ']');
    }

    /** @return {@link Set} of cache IDs participating in CDC. */
    // TODO Remove.
    public Set<Integer> getCacheIds() {
        Set<Integer> cacheIds = new HashSet<>(this.cacheIds) ;

        cacheIds.addAll(cacheRegexIds);

        return cacheIds;
    }

    /** */
    private boolean matchesRegex(String cacheName) {
        if (excludePtrn != null && excludePtrn.matcher(cacheName).matches())
            return false;

        return includePtrn != null && includePtrn.matcher(cacheName).matches();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CachesPredicate [caches=" + caches + ", includeRegex=" + includeRegex +
            ", " + "excludeRegex=" + excludeRegex + ']';
    }
}
