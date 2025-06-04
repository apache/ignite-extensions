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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.LoggerResource;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.DFLT_IS_ONLY_PRIMARY;

/**
 * Change Data Consumer that streams all data changes to destination cluster by the provided {@link #applier}.
 *
 * @see AbstractCdcEventsApplier
 */
public abstract class AbstractIgniteCdcStreamer implements CdcConsumerEx {
    /** */
    public static final String EVTS_SENT_CNT = "EventsCount";

    /** */
    public static final String EVTS_SENT_CNT_DESC = "Count of messages applied to destination cluster";

    /** */
    public static final String TYPES_SENT_CNT = "TypesCount";

    /** */
    public static final String TYPES_SENT_CNT_DESC = "Count of binary types events applied to destination cluster";

    /** */
    public static final String MAPPINGS_SENT_CNT = "MappingsCount";

    /** */
    public static final String MAPPINGS_SENT_CNT_DESC = "Count of mappings events applied to destination cluster";

    /** */
    public static final String LAST_EVT_SENT_TIME = "LastEventTime";

    /** */
    public static final String LAST_EVT_SENT_TIME_DESC = "Timestamp of last applied event to destination cluster";

    /** File with saved names of caches added by cache masks. */
    private static final String SAVED_CACHES_FILE = "caches";

    /** Temporary file with saved names of caches added by cache masks. */
    private static final String SAVED_CACHES_TMP_FILE = "caches_tmp";

    /** CDC directory path. */
    private Path cdcDir;

    /** Handle only primary entry flag. */
    private boolean onlyPrimary = DFLT_IS_ONLY_PRIMARY;

    /** Cache names. */
    private Set<String> caches;

    /** Include regex templates for cache names. */
    private Set<String> includeTemplates = new HashSet<>();

    /** Compiled include regex patterns for cache names. */
    private Set<Pattern> includeFilters;

    /** Exclude regex templates for cache names. */
    private Set<String> excludeTemplates = new HashSet<>();

    /** Compiled exclude regex patterns for cache names. */
    private Set<Pattern> excludeFilters;

    /** Cache IDs. */
    protected Set<Integer> cachesIds;

    /** Maximum batch size. */
    protected int maxBatchSize;

    /** Events applier. */
    protected AbstractCdcEventsApplier<?, ?> applier;

    /** Timestamp of last sent message. */
    protected AtomicLongMetric lastEvtTs;

    /** Count of events applied to destination cluster. */
    protected AtomicLongMetric evtsCnt;

    /** Count of binary types applied to destination cluster. */
    protected AtomicLongMetric typesCnt;

    /** Count of mappings applied to destination cluster. */
    protected AtomicLongMetric mappingsCnt;

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry reg) {
        //No-op
    }

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry reg, Path cdcDir) {
        A.notEmpty(caches, "caches");

        this.cdcDir = cdcDir;

        cachesIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());

        prepareRegexFilters();

        try {
            loadCaches().stream()
                .filter(this::matchesFilters)
                .map(CU::cacheId)
                .forEach(cachesIds::add);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        MetricRegistryImpl mreg = (MetricRegistryImpl)reg;

        this.evtsCnt = mreg.longMetric(EVTS_SENT_CNT, EVTS_SENT_CNT_DESC);
        this.typesCnt = mreg.longMetric(TYPES_SENT_CNT, TYPES_SENT_CNT_DESC);
        this.mappingsCnt = mreg.longMetric(MAPPINGS_SENT_CNT, MAPPINGS_SENT_CNT_DESC);
        this.lastEvtTs = mreg.longMetric(LAST_EVT_SENT_TIME, LAST_EVT_SENT_TIME_DESC);
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> events) {
        try {
            long msgsSnt = applier.apply(() -> F.iterator(
                events,
                F.identity(),
                true,
                evt -> !onlyPrimary || evt.primary(),
                evt -> F.isEmpty(cachesIds) || cachesIds.contains(evt.cacheId()),
                evt -> evt.version().otherClusterVersion() == null));

            if (msgsSnt > 0) {
                evtsCnt.add(msgsSnt);
                lastEvtTs.value(System.currentTimeMillis());

                if (log.isInfoEnabled())
                    log.info("Events applied [evtsApplied=" + evtsCnt.value() + ']');
            }

            return true;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
        cacheEvents.forEachRemaining(e -> {
            matchWithRegexTemplates(e.configuration().getName());
        });
    }

    /**
     * Finds match between cache name and user's regex templates.
     * If match is found, adds this cache's id to id's list and saves cache name to file.
     *
     * @param cacheName Cache name.
     */
    private void matchWithRegexTemplates(String cacheName) {
        int cacheId = CU.cacheId(cacheName);

        if (!cachesIds.contains(cacheId) && matchesFilters(cacheName)) {
            cachesIds.add(cacheId);

            try {
                List<String> caches = loadCaches();

                caches.add(cacheName);

                save(caches);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }

            if (log.isInfoEnabled())
                log.info("Cache has been added to replication [cacheName=" + cacheName + "]");
        }
    }

    /**
     * Writes caches list to file
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
     * Loads saved caches from file.
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
     * Compiles regex patterns from user templates.
     *
     * @throws PatternSyntaxException If the template's syntax is invalid
     */
    private void prepareRegexFilters() {
        includeFilters = includeTemplates.stream()
            .map(Pattern::compile)
            .collect(Collectors.toSet());

        excludeFilters = excludeTemplates.stream()
            .map(Pattern::compile)
            .collect(Collectors.toSet());
    }

    /**
     * Matches cache name with compiled regex patterns.
     *
     * @param cacheName Cache name.
     * @return True if cache name match include patterns and don't match exclude patterns.
     */
    private boolean matchesFilters(String cacheName) {
        boolean matchesInclude = includeFilters.stream()
            .anyMatch(pattern -> pattern.matcher(cacheName).matches());

        boolean notMatchesExclude = excludeFilters.stream()
            .noneMatch(pattern -> pattern.matcher(cacheName).matches());

        return matchesInclude && notMatchesExclude;
    }

    /** {@inheritDoc} */
    @Override public void onCacheDestroy(Iterator<Integer> caches) {
        caches.forEachRemaining(this::deleteRegexpCacheIfPresent);
    }

    /**
     * Removes cache added by regexp from cache list, if this cache is present in file, to prevent file size overflow.
     *
     * @param cacheId Cache id.
     */
    private void deleteRegexpCacheIfPresent(Integer cacheId) {
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

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {
        mappings.forEachRemaining(mapping -> {
            registerMapping(binaryContext(), log, mapping);

            mappingsCnt.increment();
        });

        lastEvtTs.value(System.currentTimeMillis());
    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {
        types.forEachRemaining(t -> {
            BinaryMetadata meta = ((BinaryTypeImpl)t).metadata();

            registerBinaryMeta(binaryContext(), log, meta);

            typesCnt.increment();
        });

        lastEvtTs.value(System.currentTimeMillis());
    }

    /**
     * Register {@code meta}.
     *
     * @param ctx Binary context.
     * @param log Logger.
     * @param meta Binary metadata to register.
     */
    public static void registerBinaryMeta(BinaryContext ctx, IgniteLogger log, BinaryMetadata meta) {
        ctx.updateMetadata(meta.typeId(), meta, false);

        if (log.isInfoEnabled())
            log.info("BinaryMeta [meta=" + meta + ']');
    }

    /**
     * Register {@code mapping}.
     *
     * @param ctx Binary context.
     * @param log Logger.
     * @param mapping Type mapping to register.
     */
    public static void registerMapping(BinaryContext ctx, IgniteLogger log, TypeMapping mapping) {
        assert mapping.platformType().ordinal() <= Byte.MAX_VALUE;

        byte platformType = (byte)mapping.platformType().ordinal();

        ctx.registerUserClassName(mapping.typeId(), mapping.typeName(), false, false, platformType);

        if (log.isInfoEnabled())
            log.info("Mapping [mapping=" + mapping + ']');
    }

    /** @return Binary context. */
    protected abstract BinaryContext binaryContext();

    /**
     * Sets whether entries only from primary nodes should be handled.
     *
     * @param onlyPrimary Whether entries only from primary nodes should be handled.
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setOnlyPrimary(boolean onlyPrimary) {
        this.onlyPrimary = onlyPrimary;

        return this;
    }

    /**
     * Sets cache names that participate in CDC.
     *
     * @param caches Cache names.
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setCaches(Set<String> caches) {
        this.caches = caches;

        return this;
    }

    /**
     * Sets include regex patterns that participate in CDC.
     *
     * @param includeTemplates Include regex templates
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setIncludeTemplates(Set<String> includeTemplates) {
        this.includeTemplates = includeTemplates;

        return this;
    }

    /**
     * Sets exclude regex patterns that participate in CDC.
     *
     * @param excludeTemplates Exclude regex templates
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setExcludeTemplates(Set<String> excludeTemplates) {
        this.excludeTemplates = excludeTemplates;

        return this;
    }

    /**
     * Sets maximum batch size that will be applied to destination cluster.
     *
     * @param maxBatchSize Maximum batch size.
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;

        return this;
    }
}
