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

/** Ignite to Kafka CDC metrics. */
public final class IgniteToKafkaCdcMetrics extends AbstractCdcMetrics {
    /** */
    public static final String EVTS_SENT_CNT = "EventsCount";

    /** */
    public static final String EVTS_SENT_CNT_DESC = "Count of messages sent to Kafka";

    /** */
    public static final String LAST_EVT_SENT_TIME = "LastEventTime";

    /** */
    public static final String LAST_EVT_SENT_TIME_DESC = "Timestamp of last sent event to Kafka";

    /** */
    public static final String TYPES_SENT_CNT = "TypesCount";

    /** */
    public static final String TYPES_SENT_CNT_DESC = "Count of binary types events sent to Kafka";

    /** */
    public static final String MAPPINGS_SENT_CNT = "MappingsCount";

    /** */
    public static final String MAPPINGS_SENT_CNT_DESC = "Count of mappings events sent to Kafka";

    /** Bytes sent metric name. */
    public static final String BYTES_SENT_CNT = "BytesSent";

    /** Bytes sent metric description. */
    public static final String BYTES_SENT_DESC = "Count of bytes sent to Kafka";

    /** Count of metadata markers sent name. */
    public static final String MARKERS_SENT_CNT = "MarkersCount";

    /** Count of metadata markers sent description. */
    public static final String MARKERS_SENT_CNT_DESC = "Count of metadata markers sent to Kafka";

    /** */
    private final AtomicLongMetric evtsCnt;

    /** */
    private final AtomicLongMetric lastEvtTs;

    /** */
    private final AtomicLongMetric typesCnt;

    /** */
    private final AtomicLongMetric mappingsCnt;

    /** Count of bytes sent to the Kafka. */
    private final AtomicLongMetric bytesSnt;

    /** Count of sent markers. */
    private final AtomicLongMetric markersCnt;

    /**
     * Ignite to Kafka CDC metrics constructor.
     * @param mreg Metric registry instance.
     */
    public IgniteToKafkaCdcMetrics(MetricRegistryImpl mreg) {
        this.evtsCnt = mreg.longMetric(EVTS_SENT_CNT, EVTS_SENT_CNT_DESC);
        this.typesCnt = mreg.longMetric(TYPES_SENT_CNT, TYPES_SENT_CNT_DESC);
        this.mappingsCnt = mreg.longMetric(MAPPINGS_SENT_CNT, MAPPINGS_SENT_CNT_DESC);
        this.lastEvtTs = mreg.longMetric(LAST_EVT_SENT_TIME, LAST_EVT_SENT_TIME_DESC);

        this.bytesSnt = mreg.longMetric(BYTES_SENT_CNT, BYTES_SENT_DESC);
        this.markersCnt = mreg.longMetric(MARKERS_SENT_CNT, MARKERS_SENT_CNT_DESC);
    }

    /** {@inheritDoc} */
    @Override public long getEventsSentCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void addEventsSentCount(long cnt) {

    }

    /** {@inheritDoc} */
    @Override public void setLastEventSentTime() {
        lastEvtTs.value(System.currentTimeMillis());
    }

    /** @return events sent count metric. */
    public AtomicLongMetric getEventsSentCountMetric() {
        return evtsCnt;
    }

    /** @return mapping sent count metric. */
    public AtomicLongMetric getMappingsSentCountMetric() {
        return mappingsCnt;
    }

    /** @return types sent count metric. */
    public AtomicLongMetric getTypesSentCountMetric() {
        return typesCnt;
    }

    /** @return markers sent count metric. */
    public AtomicLongMetric getMarkersSentCountMetric() {
        return markersCnt;
    }

    /**
     * Adds bytes sent count.
     * @param cnt Count.
     */
    public void addBytesSentCount(long cnt) {
        bytesSnt.add(cnt);
    }
}
