package org.apache.storm.streams.operations;

import org.apache.storm.streams.Pair;

public class PairValueJoiner<V1, V2> implements ValueJoiner<V1, V2, Pair<V1, V2>> {
    @Override
    public Pair<V1, V2> apply(V1 value1, V2 value2) {
        return new Pair<>(value1, value2);
    }
}
