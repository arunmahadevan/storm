package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Predicate;

public class FilterProcessor<T> extends BaseProcessor<T> {
    private final Predicate<T> predicate;

    public FilterProcessor(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public void execute(T input) {
        if (predicate.test(input)) {
            context.forward(input);
        }
    }
}
