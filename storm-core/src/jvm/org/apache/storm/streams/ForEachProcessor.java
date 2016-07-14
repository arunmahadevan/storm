package org.apache.storm.streams;

class ForEachProcessor<T> extends BaseProcessor<T> {
    private final Consumer<T> consumer;

    ForEachProcessor(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void execute(T input) {
        consumer.accept(input);
    }
}
