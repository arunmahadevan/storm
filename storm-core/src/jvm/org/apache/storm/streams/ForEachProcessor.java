package org.apache.storm.streams;

class ForEachProcessor<T> implements Processor<T> {
    private final Consumer<T> consumer;

    ForEachProcessor(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void execute(T input, ProcessorContext context) {
        consumer.accept(input);
    }
}
