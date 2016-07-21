package org.apache.storm.streams;

class PeekProcessor<T> extends BaseProcessor<T> {
    private final Consumer<T> consumer;

    PeekProcessor(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void execute(T input) {
        consumer.accept(input);
        context.forward(input);
    }
}
