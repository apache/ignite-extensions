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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Contains logic to process user's regexp patterns for CDC.
 */
public class CdcRegexManager {

    /** File with saved names of caches added by cache masks. */
    private static final String SAVED_CACHES_FILE = "caches";

    /** Temporary file with saved names of caches added by cache masks. */
    private static final String SAVED_CACHES_TMP_FILE = "caches_tmp";

    /** CDC directory path. */
    private final Path cdcDir;

    /** Include regex pattern for cache names. */
    private Pattern includeFilter;

    /** Exclude regex pattern for cache names. */
    private Pattern excludeFilter;

    /** Logger. */
    private IgniteLogger log;

    /**
     *
     * @param cdcDir Path to Change Data Capture Directory.
     * @param log Logger.
     */
    public CdcRegexManager(Path cdcDir, IgniteLogger log) {
        this.cdcDir = cdcDir;
        this.log = log;
    }

    /**
     * Finds and processes match between cache name and user's regexp patterns.
     *
     * @param cacheName Cache name.
     * @return True if cache name matches user's regexp patterns.
     */
    public boolean match(String cacheName) {
        return matchAndSave(Collections.singletonList(cacheName));
    }

    /**
     * Finds and processes match between a set of cache names and user's regexp patterns.
     *
     * @param cacheNames Cache names.
     * @return True if cache name matches user's regexp patterns.
     */
    public boolean match(List<String> cacheNames) {
        return matchAndSave(cacheNames);
    }

    /**
     * Get actual list of names of caches added by regex templates from cache list file.<br/>
     * All new caches started during the work of CDC application are saved to file so they can be added to CDC later if
     * appropriate regex filter is set.
     *
     * @return Caches names list.
     */
    public List<String> getSavedCaches() {
        try {
            return loadCaches().stream()
                .filter(this::matchesFilters)
                .collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Finds match between caches names and user's regex templates and saves cache name to a file.
     *
     * @param cacheNames Cache names.
     * @return True if cache name matches user's regexp patterns.
     */
    private boolean matchAndSave(List<String> cacheNames) {
        try {
            List<String> caches = loadCaches();

            caches.addAll(cacheNames);

            save(caches);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        List<String> matchingCaches = cacheNames.stream()
            .filter(this::matchesFilters)
            .collect(Collectors.toList());

        if (matchingCaches.isEmpty())
            return false;

        if (log.isInfoEnabled())
            log.info("Cache(s) has been added to replication [cacheNames=" + matchingCaches + "]");

        return true;
    }

    /**
     * Matches cache name with compiled regex patterns.
     *
     * @param cacheName Cache name.
     * @return True if cache name matches include pattern and doesn't match exclude pattern.
     */
    private boolean matchesFilters(String cacheName) {
        return includeFilter.matcher(cacheName).matches() && !excludeFilter.matcher(cacheName).matches();
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
            includeFilter = Pattern.compile(includeTemplate);

            excludeFilter = Pattern.compile(excludeTemplate);
        }
        catch (PatternSyntaxException e) {
            throw new IgniteException("Invalid cache regexp template.", e);
        }
    }

    /**
     * Loads saved CDC caches from file. If file not found, creates a new one containing empty list.
     *
     * @return List of saved caches names.
     */
    private List<String> loadCaches() throws IOException {
        if (cdcDir == null) {
            throw new IgniteException("Can't load '" + SAVED_CACHES_FILE + "' file. Cdc directory is null");
        }
        Path savedCachesPath = cdcDir.resolve(SAVED_CACHES_FILE);

        if (Files.notExists(savedCachesPath)) {
            Files.createFile(savedCachesPath);

            if (log.isInfoEnabled())
                log.info("Cache list created: " + savedCachesPath);
        }

        return Files.readAllLines(savedCachesPath);
    }

    /**
     * Writes caches list to file.
     *
     * @param caches Caches list.
     */
    private void save(List<String> caches) throws IOException {
        if (cdcDir == null) {
            throw new IgniteException("Can't write to '" + SAVED_CACHES_FILE + "' file. Cdc directory is null");
        }
        Path savedCachesPath = cdcDir.resolve(SAVED_CACHES_FILE);
        Path tmpSavedCachesPath = cdcDir.resolve(SAVED_CACHES_TMP_FILE);

        StringBuilder cacheList = new StringBuilder();

        for (String cache : caches) {
            cacheList.append(cache);

            cacheList.append('\n');
        }

        Files.write(tmpSavedCachesPath, cacheList.toString().getBytes());

        Files.move(tmpSavedCachesPath, savedCachesPath, ATOMIC_MOVE, REPLACE_EXISTING);
    }

    /**
     * Removes cache added by regexp from cache list if such cache is present in file to prevent disk space overflow.
     *
     * @param cacheId Cache id.
     */
    public void deleteRegexpCacheIfPresent(Integer cacheId) {
        try {
            List<String> caches = loadCaches();

            Optional<String> cacheName = caches.stream()
                .filter(name -> CU.cacheId(name) == cacheId)
                .findAny();

            if (cacheName.isPresent()) {
                String name = cacheName.get();

                caches.remove(name);

                save(caches);

                if (log.isInfoEnabled())
                    log.info("Cache has been removed from replication [cacheName=" + name + ']');
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}
