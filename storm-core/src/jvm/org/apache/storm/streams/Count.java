package org.apache.storm.streams;

public class Count<T> implements Aggregator<T, Long> {
    @Override
    public Long init() {
        return 0L;
    }

    @Override
    public Long apply(T value, Long aggregate) {
        return aggregate + 1;
    }
}
