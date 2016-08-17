package org.apache.storm.streams.operations;

public class IdentityFunction<T> implements Function<T, T> {

    @Override
    public T apply(T input) {
        return input;
    }
}
