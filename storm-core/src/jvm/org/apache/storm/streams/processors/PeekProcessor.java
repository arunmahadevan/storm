package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Consumer;

public class PeekProcessor<T> extends BaseProcessor<T> {
    private final Consumer<T> consumer;

    public PeekProcessor(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void execute(T input) {
        consumer.accept(input);
        context.forward(input);
    }
}
