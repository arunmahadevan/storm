package org.apache.storm.streams;

class MapValuesProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> {
    private final Function<V, R> function;

    MapValuesProcessor(Function<V, R> function) {
        this.function = function;
    }

    @Override
    public void execute(Pair<K, V> input) {
        context.forward(new Pair<>(input.getFirst(), function.apply(input.getSecond())));
    }
}
