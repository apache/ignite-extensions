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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;

/** Metric Holder DTO for CDC. */
public class MetricsHolder {
    /** Metric map for custom set of metrics registered. */
    private final ConcurrentHashMap<String, AbstractMetric> metricMap;

    /** Metric holder constructor. */
    public MetricsHolder() {
        metricMap = new ConcurrentHashMap<>();
    }

    /**
     * Adds metric to the holder.
     * @param name Metric name.
     * @param desc Metric Description.
     * @param metricFunc Metric register.
     * @return {@link MetricsHolder} this
     */
    public MetricsHolder addMetric(
        String name,
        String desc,
        BiFunction<String, String, AtomicLongMetric> metricFunc
    ) {
        metricMap.putIfAbsent(name, metricFunc.apply(name, desc));

        return this;
    }

    /**
     * @param name Name.
     * @param cls {@link AbstractMetric} class.
     * @param <T> {@link AbstractMetric} type.
     * @return {@link AbstractMetric} with specified name.
     */
    public <T extends AbstractMetric> T getMetric(String name, Class<T> cls) {
        return cls.cast(metricMap.get(name));
    }
}
