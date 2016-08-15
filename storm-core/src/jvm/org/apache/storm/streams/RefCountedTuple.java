package org.apache.storm.streams;

import org.apache.storm.tuple.Tuple;

public class RefCountedTuple {
    private int count = 0;
    private final Tuple tuple;

    public RefCountedTuple(Tuple tuple) {
        this.tuple = tuple;
    }

    public boolean shouldAck() {
        return count == 0;
    }

    public void increment() {
        ++count;
    }

    public void decrement() {
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
