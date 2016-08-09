package org.apache.storm.streams;

/**
 * A function that accepts one argument and returns an {@link Iterable} of {@link Pair} as its result.
 *
 * @param <T> the type of the input to the function
 * @param <K> the key type of the key-value pairs produced as a result
 * @param <V> the value type of the key-value pairs produced as a result
 */
public interface PairFlatMapFunction<T, K, V> extends FlatMapFunction<T, Pair<K, V>> {
}
