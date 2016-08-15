package org.apache.storm.streams.operations;

/**
 * Interface for aggregating values.
 *
 * @param <T> the original value type
 * @param <R> the aggregated value type
 */
public interface Aggregator<T, R> extends Operation {
    /**
     * The initial value of the aggregate to start with.
     *
     * @return the initial value
     */
    R init();

    /**
     * Returns a new aggregate by applying the value with the current aggregate.
     *
     * @param value     the value to aggregate
     * @param aggregate the current aggregate
     * @return the new aggregate
     */
    R apply(T value, R aggregate);
}
