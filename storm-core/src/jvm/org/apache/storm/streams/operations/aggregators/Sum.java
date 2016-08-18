package org.apache.storm.streams.operations.aggregators;

import org.apache.storm.streams.operations.Aggregator;

/**
 * Computes the long sum of the input values
 */
public class Sum implements Aggregator<Number, Long> {
    @Override
    public Long init() {
        return 0L;
    }

    @Override
    public Long apply(Number value, Long aggregate) {
        return value.longValue() + aggregate;
    }
}
