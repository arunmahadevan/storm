package org.apache.storm.streams;

class FilterProcessor<T> implements Processor<T> {
    private Predicate<T> predicate;

    FilterProcessor(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public void execute(T input, ProcessorContext context) {
        if (predicate.test(input)) {
            context.forward(input);
        }
    }
}
