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
