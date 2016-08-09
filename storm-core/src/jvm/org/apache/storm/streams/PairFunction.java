package org.apache.storm.streams;

/**
 * A function that accepts an argument and produces a key-value Pair.
 *
 * @param <T> the type of the input to the function
 * @param <K> the key type
 * @param <V> the value type
 */
public interface PairFunction<T, K, V> extends Function<T, Pair<K,V>> {
}
