package org.apache.storm.streams.operations;

/**
 * Represents a predicate (boolean-valued function) of a value.
 *
 * @param <T> the value type
 */
public interface Predicate<T> extends Operation {
    /**
     * Evaluates this predicate on the given argument.
     *
     * @param input the input argument
     * @return true if the input matches the predicate, false otherwise
     */
    boolean test(T input);
}
