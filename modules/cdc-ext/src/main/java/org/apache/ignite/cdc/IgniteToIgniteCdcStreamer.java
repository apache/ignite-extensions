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

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.LoggerResource;

/**
 * Change Data Consumer that streams all data changes to provided {@link #dest} Ignite cluster.
 * Consumer will just fail in case of any error during write. Fail of consumer will lead to the fail of {@code ignite-cdc} application.
 * It expected that {@code ignite-cdc} will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka unavailability or network issues.
 *
 * If you have plans to apply written messages to the other Ignite cluster in active-active manner,
 * e.g. concurrent updates of the same entry in other cluster is possible,
 * please, be aware of {@link CacheVersionConflictResolverImpl} conflict resolved.
 * Configuration of {@link CacheVersionConflictResolverImpl} can be found in {@link KafkaToIgniteCdcStreamer} documentation.
 *
 * @see CdcMain
 * @see CacheVersionConflictResolverImpl
 */
public class IgniteToIgniteCdcStreamer extends CdcEventsApplier implements CdcConsumer {
    /** */
    public static final String EVTS_CNT = "EventsCount";

    /** */
    public static final String EVTS_CNT_DESC = "Count of messages applied to destination cluster";

    /** */
    public static final String LAST_EVT_TIME = "LastEventTime";

    /** */
    public static final String LAST_EVT_TIME_DESC = "Timestamp of last applied event";

    /** Destination cluster client configuration. */
    private final IgniteConfiguration destIgniteCfg;

    /** Handle only primary entry flag. */
    private final boolean onlyPrimary;

    /** Destination Ignite cluster client */
    private IgniteEx dest;

    /** Timestamp of last sent message. */
    private AtomicLongMetric lastEvtTs;

    /** Count of events applied to destination cluster. */
    protected AtomicLongMetric evtsCnt;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache IDs. */
    private final Set<Integer> cachesIds;

    /**
     * @param destIgniteCfg Configuration of the destination Ignite node.
     * @param onlyPrimary Only primary flag.
     * @param caches Cache names.
     * @param maxBatchSize Maximum batch size.
     */
    public IgniteToIgniteCdcStreamer(IgniteConfiguration destIgniteCfg, boolean onlyPrimary, Set<String> caches, int maxBatchSize) {
        super(maxBatchSize);

        this.destIgniteCfg = destIgniteCfg;
        this.onlyPrimary = onlyPrimary;

        cachesIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        if (log.isInfoEnabled())
            log.info("Ignite To Ignite Streamer [cacheIds=" + cachesIds + ']');

        dest = (IgniteEx)Ignition.start(destIgniteCfg);

        this.evtsCnt = mreg.longMetric(EVTS_CNT, EVTS_CNT_DESC);
        this.lastEvtTs = mreg.longMetric(LAST_EVT_TIME, LAST_EVT_TIME_DESC);
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> evts) {
        try {
            long msgsSnt0 = apply(() -> F.iterator(
                evts,
                F.identity(),
                true,
                evt -> !onlyPrimary || evt.primary(),
                evt -> F.isEmpty(cachesIds) || cachesIds.contains(evt.cacheId()),
                evt -> evt.version().otherClusterVersion() == null));

            if (msgsSnt0 > 0) {
                evtsCnt.add(msgsSnt0);
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
    @Override public void stop() {
        dest.close();
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx ignite() {
        return dest;
    }

    /** {@inheritDoc} */
    @Override protected IgniteLogger log() {
        return log;
    }
}
