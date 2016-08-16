package org.apache.storm.streams.processors;

import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.FlatMapFunction;

public class FlatMapValuesProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> {
    private final FlatMapFunction<V, R> function;

    public FlatMapValuesProcessor(FlatMapFunction<V, R> function) {
        this.function = function;
    }

    @Override
    protected void execute(Pair<K, V> input) {
        Iterable<R> it = function.apply(input.getSecond());
        for (R res : it) {
            context.forward(new Pair<>(input.getFirst(), res));
        }
    }
}
