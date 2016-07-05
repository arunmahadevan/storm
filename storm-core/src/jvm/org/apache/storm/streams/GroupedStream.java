package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

public class GroupedStream<T> {
    public <R> GroupedStream<Pair<T, R>> aggregate(Aggregator<T, R> aggregator, Fields outputFields) {
        return null;
    }
}
