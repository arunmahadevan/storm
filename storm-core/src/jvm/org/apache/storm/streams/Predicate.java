package org.apache.storm.streams;

public interface Predicate<T> extends Operation {
    boolean test(T input);
}
