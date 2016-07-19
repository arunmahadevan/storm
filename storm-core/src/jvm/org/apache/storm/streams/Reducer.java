package org.apache.storm.streams;

public interface Reducer<T> extends Operation {
    T apply (T val1, T val2);
}
