package org.apache.storm.streams;

public interface Function<T, R> extends Operation {
    R apply(T input);
}
