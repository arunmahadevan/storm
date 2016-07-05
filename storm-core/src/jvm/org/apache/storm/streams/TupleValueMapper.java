package org.apache.storm.streams;

import org.apache.storm.tuple.Tuple;

public interface TupleValueMapper<T> extends Function<Tuple, T> {
}
