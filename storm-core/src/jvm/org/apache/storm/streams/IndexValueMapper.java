package org.apache.storm.streams;

import org.apache.storm.tuple.Tuple;

public class IndexValueMapper<T> implements TupleValueMapper<T> {
    final int index;

    public IndexValueMapper(int index) {
        this.index = index;
    }

    @Override
    public T apply(Tuple input) {
        return (T) input.getValue(index);
    }
}
