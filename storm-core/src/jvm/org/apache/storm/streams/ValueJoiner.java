package org.apache.storm.streams;

interface ValueJoiner<V1, V2, R> extends Operation {
    R apply(V1 value1, V2 value2);
}
