package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.Pair;

public class MapValuesProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> {
    private final Function<V, R> function;

    public MapValuesProcessor(Function<V, R> function) {
        this.function = function;
    }

    @Override
    public void execute(Pair<K, V> input) {
        context.forward(new Pair<>(input.getFirst(), function.apply(input.getSecond())));
    }
}
