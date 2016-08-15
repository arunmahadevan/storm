package org.apache.storm.streams.operations;

import org.apache.storm.tuple.Tuple;

public interface TupleValueMapper<T> extends Function<Tuple, T> {
}
