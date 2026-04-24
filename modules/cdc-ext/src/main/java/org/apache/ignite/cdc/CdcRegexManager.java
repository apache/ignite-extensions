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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;

/**
 * Contains logic to process user's regexp patterns for CDC.
 */
public class CdcRegexManager {

    /** Include regex pattern for cache names. */
    private Pattern includeFilter;

    /** Exclude regex pattern for cache names. */
    private Pattern excludeFilter;

    /** Logger. */
    private IgniteLogger log;

    /**
     *
     * @param log Logger.
     */
    public CdcRegexManager(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Matches cache name with compiled regex patterns.
     *
     * @param cacheName Cache name.
     * @return True if cache name matches include pattern and doesn't match exclude pattern.
     */
    public boolean matchesFilters(String cacheName) {
        if (includeFilter.matcher(cacheName).matches() && excludeFilter.matcher(cacheName).matches()) {
            if (log.isInfoEnabled())
                log.info("Cache name matches both include and exclude regexp templates. Will except this cache from " +
                    "replication [cacheName=" + cacheName + ", includeTemplate=" + includeFilter + ", excludeTemplate=" +
                    excludeFilter + "]");

            return false;
        }
        return includeFilter.matcher(cacheName).matches();
    }

    /**
     * Compiles regex patterns from user templates.
     *
     * @param includeTemplate Include regex template.
     * @param excludeTemplate Exclude regex template.
     * @throws IgniteException If the template's syntax is invalid
     */
    public void compileRegexp(String includeTemplate, String excludeTemplate) {
        try {
            includeFilter = includeTemplate != null ? Pattern.compile(includeTemplate) : Pattern.compile("");

            excludeFilter = excludeTemplate != null ? Pattern.compile(excludeTemplate) : Pattern.compile("");
        }
        catch (PatternSyntaxException e) {
            throw new IgniteException("Invalid cache regexp template", e);
        }
    }
}
