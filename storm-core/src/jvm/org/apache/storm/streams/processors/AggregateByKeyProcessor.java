package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Aggregator;
import org.apache.storm.streams.Pair;

import java.util.HashMap;
import java.util.Map;

public class AggregateByKeyProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> implements BatchProcessor {
    private final Aggregator<V, R> aggregator;
    private final Map<K, R> state = new HashMap<>();

    public AggregateByKeyProcessor(Aggregator<V, R> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public void execute(Pair<K, V> input) {
        K key = input.getFirst();
        V val = input.getSecond();
        R agg = state.get(key);
        if (agg == null) {
            agg = aggregator.init();
        }
        state.put(key, aggregator.apply(val, agg));
        mayBeForwardAggUpdate(new Pair<>(key, state.get(key)));
    }

    @Override
    public void finish() {
        for (Map.Entry<K, R> entry : state.entrySet()) {
            context.forward(new Pair<>(entry.getKey(), entry.getValue()));
        }
        state.clear();
    }

}
