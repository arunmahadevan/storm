package org.apache.storm.streams;

import org.apache.storm.tuple.Tuple;

class RefCountedTuple {
    private int count = 0;
    private final Tuple tuple;

    RefCountedTuple(Tuple tuple) {
        this.tuple = tuple;
    }

    boolean shouldAck() {
        return count == 0;
    }

    void increment() {
        ++count;
    }

    void decrement() {
        --count;
    }

    public Tuple tuple() {
        return tuple;
    }

    @Override
    public String toString() {
        return "RefCountedTuple{" +
                "count=" + count +
                ", tuple=" + tuple +
                '}';
    }
}
