package org.apache.storm.streams;

public interface Consumer<T> extends Operation {
    void accept(T input);
}
