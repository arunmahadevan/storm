package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Function;

public class MapProcessor<T, R> extends BaseProcessor<T> {
    private final Function<T, R> function;

    public MapProcessor(Function<T, R> function) {
        this.function = function;
    }

    @Override
    public void execute(T input) {
        context.forward(function.apply(input));
    }
}
