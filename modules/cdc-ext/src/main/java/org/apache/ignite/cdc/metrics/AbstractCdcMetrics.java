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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/** Abstract class for CDC metrics. */
public abstract class AbstractCdcMetrics {
    /** */
    public static final String PUT_ALL_TIME = "PutAllTime";

    /** */
    public static final String PUT_ALL_TIME_DESC =
        "PutAll time for which this CDC client is the initiator, in nanoseconds.";

    /** */
    public static final String REMOVE_ALL_TIME = "RemoveAllTime";

    /** */
    public static final String REMOVE_ALL_TIME_DESC =
        "RemoveAll time for which this CDC client is the initiator, in nanoseconds.";

    /** */
    public static final String PUT_TIME_TOTAL = "PutTimeTotal";

    /** */
    public static final String PUT_TIME_TOTAL_DESC =
        "The total time of cache puts for which this CDC client is the initiator, in nanoseconds.";

    /** */
    public static final String REMOVE_TIME_TOTAL = "RemoveTimeTotal";

    /** */
    public static final String REMOVE_TIME_TOTAL_DESC =
        "The total time of cache removal for which this CDC client is the initiator, in nanoseconds.";

    /** Histogram buckets for duration get, put, remove, commit, rollback operations in nanoseconds. */
    public static final long[] HISTOGRAM_BUCKETS = new long[] {
        NANOSECONDS.convert(1, MILLISECONDS),
        NANOSECONDS.convert(10, MILLISECONDS),
        NANOSECONDS.convert(100, MILLISECONDS),
        NANOSECONDS.convert(250, MILLISECONDS),
        NANOSECONDS.convert(1000, MILLISECONDS)
    };

    /** @return events sent count. */
    public abstract long getEventsSentCount();

    /**
     * Updates events sent count.
     * @param cnt Count.
     */
    public abstract void addEventsSentCount(long cnt);

    /** Updates last event sent time. */
    public abstract void setLastEventSentTime();

    /**
     * Increments the putAllConflict time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutAllTimeNanos(long duration) {
        // No-op.
    }

    /**
     * Increments the removeAllConflict time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAllTimeNanos(long duration) {
        // No-op.
    }
}
