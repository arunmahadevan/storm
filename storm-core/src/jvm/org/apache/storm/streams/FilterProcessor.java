package org.apache.storm.streams;

class FilterProcessor<T> extends BaseProcessor<T> {
    private Predicate<T> predicate;

    FilterProcessor(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public void execute(T input) {
        if (predicate.test(input)) {
            context.forward(input);
        }
    }
}
