package org.apache.storm.streams;

/**
 * Represents a function that accepts one argument and produces a result.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
public interface Function<T, R> extends Operation {
    /**
     * Applies this function to the given argument.
     *
     * @param input the input to the function
     * @return the function result
     */
    R apply(T input);
}
