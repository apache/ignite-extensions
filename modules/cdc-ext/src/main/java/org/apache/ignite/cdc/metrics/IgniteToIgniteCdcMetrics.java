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

package org.apache.ignite.cdc.metrics;

import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;

/** Ignite to Ignite CDC metrics. */
public final class IgniteToIgniteCdcMetrics extends AbstractCdcMetrics {
    /** */
    public static final String EVTS_SENT_CNT = "EventsCount";

    /** */
    public static final String EVTS_SENT_CNT_DESC = "Count of messages applied to destination cluster";

    /** */
    public static final String LAST_EVT_SENT_TIME = "LastEventTime";

    /** */
    public static final String LAST_EVT_SENT_TIME_DESC = "Timestamp of last applied event to destination cluster";

    /** */
    public static final String TYPES_SENT_CNT = "TypesCount";

    /** */
    public static final String TYPES_SENT_CNT_DESC = "Count of binary types events applied to destination cluster";

    /** */
    public static final String MAPPINGS_SENT_CNT = "MappingsCount";

    /** */
    public static final String MAPPINGS_SENT_CNT_DESC = "Count of mappings events applied to destination cluster";

    /** */
    private final AtomicLongMetric evtsCnt;

    /** */
    private final AtomicLongMetric lastEvtTs;

    /** */
    private final AtomicLongMetric typesCnt;

    /** */
    private final AtomicLongMetric mappingsCnt;

    /** */
    private final HistogramMetricImpl putAllTime;

    /** */
    private final HistogramMetricImpl rmvAllTime;

    /** Total put time taken nanos. */
    private final AtomicLongMetric putTimeTotal;

    /** Total remove time taken nanos. */
    private final AtomicLongMetric rmvTimeTotal;

    /**
     * Ignite to Ignite CDC metrics constructor.
     * @param mreg {@link MetricRegistryImpl} instance.
     */
    public IgniteToIgniteCdcMetrics(MetricRegistryImpl mreg) {
        this.evtsCnt = mreg.longMetric(EVTS_SENT_CNT, EVTS_SENT_CNT_DESC);
        this.typesCnt = mreg.longMetric(TYPES_SENT_CNT, TYPES_SENT_CNT_DESC);
        this.mappingsCnt = mreg.longMetric(MAPPINGS_SENT_CNT, MAPPINGS_SENT_CNT_DESC);
        this.lastEvtTs = mreg.longMetric(LAST_EVT_SENT_TIME, LAST_EVT_SENT_TIME_DESC);

        this.putAllTime = mreg.histogram(PUT_ALL_TIME, HISTOGRAM_BUCKETS, PUT_ALL_TIME_DESC);
        this.rmvAllTime = mreg.histogram(REMOVE_ALL_TIME, HISTOGRAM_BUCKETS, REMOVE_ALL_TIME_DESC);
        this.putTimeTotal = mreg.longMetric(PUT_TIME_TOTAL, PUT_TIME_TOTAL_DESC);
        this.rmvTimeTotal = mreg.longMetric(REMOVE_TIME_TOTAL, REMOVE_TIME_TOTAL_DESC);
    }

    /** {@inheritDoc} */
    @Override public long getEventsSentCount() {
        return evtsCnt.value();
    }

    /** {@inheritDoc} */
    @Override public void addEventsSentCount(long cnt) {
        evtsCnt.add(cnt);
    }

    /** {@inheritDoc} */
    @Override public void setLastEventSentTime() {
        lastEvtTs.value(System.currentTimeMillis());
    }

    /** {@inheritDoc} */
    @Override public void addPutAllTimeNanos(long duration) {
        putTimeTotal.add(duration);

        putAllTime.value(duration);
    }

    /** {@inheritDoc} */
    @Override public void addRemoveAllTimeNanos(long duration) {
        rmvTimeTotal.add(duration);

        rmvAllTime.value(duration);
    }

    /** Increments mappings sent count. */
    public void incrementMappingsSentCount() {
        mappingsCnt.increment();
    }

    /** Increments types sent count. */
    public void incrementTypesSentCount() {
        typesCnt.increment();
    }
}
