package org.apache.storm.streams;

public interface Aggregator<T, R> extends Operation {
    R init();
    R apply(T value, R aggregate);
}
