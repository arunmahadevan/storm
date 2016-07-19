package org.apache.storm.streams;

import java.util.HashMap;
import java.util.Map;

public class AggregateByKeyProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> {
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
        context.forward(new Pair<>(key, state.get(key)));
    }

    void finish() {
        for (Map.Entry<K, R> entry : state.entrySet()) {
            context.forward(new Pair<>(entry.getKey(), entry.getValue()));
        }
        state.clear();
    }

}
