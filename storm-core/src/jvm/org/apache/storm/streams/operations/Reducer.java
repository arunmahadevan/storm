package org.apache.storm.streams.operations;

/**
 * The {@link Reducer} performs an operation on two values of the same type producing a result of the same
 * type as the operands.
 *
 * @param <T> the type of the arguments and the result
 */
public interface Reducer<T> extends Operation {
    /**
     * Applies this function to the given arguments.
     *
     * @param arg1 the first argument
     * @param arg2 the second argument
     * @return
     */
    T apply(T arg1, T arg2);
}
