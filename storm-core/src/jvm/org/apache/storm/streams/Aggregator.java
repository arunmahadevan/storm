package org.apache.storm.streams;

public interface Aggregator<T, R> extends Operation {
    R apply(T value, R aggregate);
}
