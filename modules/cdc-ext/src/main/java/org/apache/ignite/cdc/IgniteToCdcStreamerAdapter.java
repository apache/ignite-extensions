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
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.DFLT_IS_ONLY_PRIMARY;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_MAX_BATCH_SIZE;

/**
 * Change Data Consumer adapter that streams all data changes to destination cluster by the provided {@link #applier}.
 *
 * @see CdcEventsApplierAdapter
 */
public abstract class IgniteToCdcStreamerAdapter<T extends IgniteToCdcStreamerAdapter> implements CdcConsumer {
    /** */
    public static final String EVTS_CNT = "EventsCount";

    /** */
    public static final String EVTS_CNT_DESC = "Count of messages applied to destination cluster";

    /** */
    public static final String LAST_EVT_TIME = "LastEventTime";

    /** */
    public static final String LAST_EVT_TIME_DESC = "Timestamp of last applied event";

    /** Handle only primary entry flag. */
    private boolean onlyPrimary = DFLT_IS_ONLY_PRIMARY;

    /** Cache names. */
    private Set<String> caches;

    /** Cache IDs. */
    protected Set<Integer> cachesIds;

    /** Maximum batch size. */
    protected int maxBatchSize = DFLT_MAX_BATCH_SIZE;

    /** Timestamp of last sent message. */
    private AtomicLongMetric lastEvtTs;

    /** Count of events applied to destination cluster. */
    protected AtomicLongMetric evtsCnt;

    /** Events applier. */
    private CdcEventsApplierAdapter applier;

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        A.notEmpty(caches, "caches");

        cachesIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());

        this.evtsCnt = mreg.longMetric(EVTS_CNT, EVTS_CNT_DESC);
        this.lastEvtTs = mreg.longMetric(LAST_EVT_TIME, LAST_EVT_TIME_DESC);
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> evts) {
        try {
            long msgsSnt = applier.apply(() -> F.iterator(
                evts,
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

    /**
     * Sets whether entries only from primary nodes should be handled.
     *
     * @param onlyPrimary Whether entries only from primary nodes should be handled.
     * @return {@code this} for chaining.
     */
    public T setOnlyPrimary(boolean onlyPrimary) {
        this.onlyPrimary = onlyPrimary;

        return (T)this;
    }

    /**
     * Sets cache names that participate in CDC.
     *
     * @param caches Cache names.
     * @return {@code this} for chaining.
     */
    public T setCaches(Set<String> caches) {
        this.caches = caches;

        return (T)this;
    }

    /**
     * Sets maximum batch size that will be applied to destination cluster.
     *
     * @param maxBatchSize Maximum batch size.
     * @return {@code this} for chaining.
     */
    public T setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;

        return (T)this;
    }

    /** Sets events applier. */
    protected void applier(CdcEventsApplierAdapter applier) {
        this.applier = applier;
    }
}
