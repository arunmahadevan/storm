package org.apache.storm.streams;

public interface PairFunction<T, K, V> extends Function<T, Pair<K,V>> {
}
