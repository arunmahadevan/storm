package org.apache.storm.streams;

public interface FlatMapFunction<T, R> extends Function<T, Iterable<R>> {
}
