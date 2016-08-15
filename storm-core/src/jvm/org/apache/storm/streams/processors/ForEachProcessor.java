package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Consumer;

public class ForEachProcessor<T> extends BaseProcessor<T> {
    private final Consumer<T> consumer;

    public ForEachProcessor(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void execute(T input) {
        consumer.accept(input);
    }
}
