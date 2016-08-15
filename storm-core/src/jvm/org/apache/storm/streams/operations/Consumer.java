package org.apache.storm.streams.operations;

/**
 * Represents an operation that accepts a single input argument and returns no result.
 *
 * @param <T> the type of the input argument
 */
public interface Consumer<T> extends Operation {
    /**
     * Performs an operation on the given argument.
     *
     * @param input the input
     */
    void accept(T input);
}
