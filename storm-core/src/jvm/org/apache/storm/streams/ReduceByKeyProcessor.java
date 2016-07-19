package org.apache.storm.streams;

import java.util.HashMap;
import java.util.Map;

public class ReduceByKeyProcessor<K, V> extends BaseProcessor<Pair<K, V>> {
    private final Reducer<V> reducer;
    private final Map<K, V> state = new HashMap<>();

    public ReduceByKeyProcessor(Reducer<V> reducer) {
        this.reducer = reducer;
    }

    @Override
    public void execute(Pair<K, V> input) {
        K key = input.getFirst();
        V val = input.getSecond();
        V agg = state.get(key);
        if (agg == null) {
            agg = val;
        } else {
            agg = reducer.apply(agg, val);
        }
        state.put(key, agg);
        context.forward(new Pair<>(key, agg));
    }

    void finish() {
        for (Map.Entry<K, V> entry : state.entrySet()) {
            context.forward(new Pair<>(entry.getKey(), entry.getValue()));
        }
        state.clear();
    }

}
