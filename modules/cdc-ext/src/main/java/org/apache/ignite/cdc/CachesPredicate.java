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

    /**
     * Sets cache ids of caches participating in CDC.
     * @param caches Cache names.
     */
    public void setCaches(Collection<String> caches) {
        cacheIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Sets include regex pattern for caches participating in CDC.
     *
     * @param includeRegex Include regex string
     * @throws IgniteException If the template's syntax is invalid
     */
    public void setIncludeCacheTemplate(String includeRegex) {
        try {
            includePtrn = includeRegex != null ? Pattern.compile(includeRegex) : Pattern.compile("");
        }
        catch (PatternSyntaxException e) {
            throw new IgniteException("Invalid cache regexp template", e);
        }
    }

    /**
     * Sets exclude regex pattern for caches participating in CDC.
     *
     * @param excludeRegex Exclude regex string
     * @throws IgniteException If the template's syntax is invalid
     */
    public void setExcludeCacheTemplate(String excludeRegex) {
        try {
            excludePtrn = excludeRegex != null ? Pattern.compile(excludeRegex) : Pattern.compile("");
        }
        catch (PatternSyntaxException e) {
            throw new IgniteException("Invalid cache regexp template", e);
        }
    }

    /**
     * @param log Logger.
     */
    public void setLog(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Integer cacheId) {
        return cacheIds.contains(cacheId) || cacheRegexIds.contains(cacheId);
    }

    /**
     * Matches cache name with compiled regex patterns.
     *
     * @param cacheName Cache name.
     * @return True if cache name matches include pattern and doesn't match exclude pattern.
     */
    public boolean onCacheEvent(String cacheName) {
        if (excludePtrn.matcher(cacheName).matches())
            return false;

        if (includePtrn.matcher(cacheName).matches() && !cacheRegexIds.contains(CU.cacheId(cacheName))) {
            cacheRegexIds.add(CU.cacheId(cacheName));

            if (log.isInfoEnabled())
                log.info("Cache [cacheName=" + cacheName + "] has been added to the replication");
        }

        return true;
    }

    /**
     * Removes destroyed cache from replication.
     * @param cacheId Cache id.
     * */
    public void onCacheDestroy(int cacheId) {
        cacheRegexIds.remove(cacheId);

        if (log.isInfoEnabled())
            log.info("Cache [cacheId=" + cacheId + "] has been removed from the replication");
    }

    /**
     * @return {@link Set} of cache ids participating in CDC.
     */
    public Set<Integer> getCacheIds() {
        Set<Integer> cacheIds = new HashSet<>(this.cacheIds) ;

        cacheIds.addAll(cacheRegexIds);

        return cacheIds;
    }
}
