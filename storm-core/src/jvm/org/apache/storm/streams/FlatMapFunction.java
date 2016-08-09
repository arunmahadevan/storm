package org.apache.storm.streams;

/**
 * A function that accepts one argument and returns an {@link Iterable} of elements as its result.
 *
 * @param <T> the type of the input to the function
 * @param <R> the result type. An iterable of this type is returned from this function
 */
public interface FlatMapFunction<T, R> extends Function<T, Iterable<R>> {
}
