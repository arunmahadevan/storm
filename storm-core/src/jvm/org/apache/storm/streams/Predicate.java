package org.apache.storm.streams;

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
     * @return true if the input matches the predicate, otherwise false
     */
    boolean test(T input);
}
