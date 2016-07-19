package org.apache.storm.streams;

public interface PairFlatMapFunction<T, K, V> extends FlatMapFunction<T, Pair<K, V>> {
}
