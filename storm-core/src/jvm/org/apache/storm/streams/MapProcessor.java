package org.apache.storm.streams;

class MapProcessor<T, R> extends BaseProcessor<T> {
    private final Function<T, R> function;

    MapProcessor(Function<T, R> function) {
        this.function = function;
    }

    @Override
    public void execute(T input) {
        context.forward(function.apply(input));
    }
}
