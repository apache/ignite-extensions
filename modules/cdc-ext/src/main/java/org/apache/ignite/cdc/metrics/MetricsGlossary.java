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

/** Glossary for CDC metrics. */
public final class MetricsGlossary {
    /**
     * Default constructor.
     */
    private MetricsGlossary() {
        // No op. constructor
    }

    /** */
    public static final String I2I_EVTS_SNT_CNT = "EventsCount";

    /** */
    public static final String I2I_EVTS_SNT_CNT_DESC = "Count of messages applied to destination cluster";

    /** */
    public static final String I2I_TYPES_SNT_CNT = "TypesCount";

    /** */
    public static final String I2I_TYPES_SNT_CNT_DESC = "Count of binary types events applied to destination cluster";

    /** */
    public static final String I2I_MAPPINGS_SNT_CNT = "MappingsCount";

    /** */
    public static final String I2I_MAPPINGS_SNT_CNT_DESC = "Count of mappings events applied to destination cluster";

    /** */
    public static final String I2I_LAST_EVT_SNT_TIME = "LastEventTime";

    /** */
    public static final String I2I_LAST_EVT_SNT_TIME_DESC = "Timestamp of last applied event to destination cluster";

    /** */
    public static final String I2K_EVTS_SNT_CNT = "EventsCount";

    /** */
    public static final String I2K_EVTS_SNT_CNT_DESC = "Count of messages applied to Kafka";

    /** */
    public static final String I2K_TYPES_SNT_CNT = "TypesCount";

    /** */
    public static final String I2K_TYPES_SNT_CNT_DESC = "Count of binary types events applied to Kafka";

    /** */
    public static final String I2K_MAPPINGS_SNT_CNT = "MappingsCount";

    /** */
    public static final String I2K_MAPPINGS_SNT_CNT_DESC = "Count of mappings events applied to Kafka";

    /** */
    public static final String I2K_LAST_EVT_SNT_TIME = "LastEventTime";

    /** */
    public static final String I2K_LAST_EVT_SNT_TIME_DESC = "Timestamp of last applied event to Kafka";

    /** Bytes sent metric name. */
    public static final String I2K_BYTES_SNT = "BytesSent";

    /** Bytes sent metric description. */
    public static final String I2K_BYTES_SNT_DESC = "Count of bytes sent to Kafka";

    /** Count of metadata markers sent name. */
    public static final String I2K_MARKERS_SNT_CNT = "MarkersCount";

    /** Count of metadata markers sent description. */
    public static final String I2K_MARKERS_SNT_CNT_DESC = "Count of metadata markers sent to Kafka";

    /** Count of events received name. */
    public static final String K2I_EVTS_RSVD_CNT = "EventsReceivedCount";

    /** Count of events received description. */
    public static final String K2I_EVTS_RSVD_CNT_DESC = "Count of events received from kafka";

    /** Timestamp of last received event name. */
    public static final String K2I_LAST_EVT_RSVD_TIME = "LastEventReceivedTime";

    /** Timestamp of last received event description. */
    public static final String K2I_LAST_EVT_RSVD_TIME_DESC = "Timestamp of last received event from kafka";

    /** Count of metadata markers received name. */
    public static final String K2I_MARKERS_RSVD_CNT = "MarkersCount";

    /** Count of metadata markers received description. */
    public static final String K2I_MARKERS_RSVD_CNT_DESC = "Count of metadata markers received from Kafka";

    /** Count of events sent name. */
    public static final String K2I_MSGS_SNT_CNT = "EventsSentCount";

    /** Count of events sent description. */
    public static final String K2I_MSGS_SNT_CNT_DESC = "Count of events sent to destination cluster";

    /** Timestamp of last sent batch name. */
    public static final String K2I_LAST_MSG_SNT_TIME = "LastBatchSentTime";

    /** Timestamp of last sent batch description. */
    public static final String K2I_LAST_MSG_SNT_TIME_DESC = "Timestamp of last sent batch to the destination cluster";
}
