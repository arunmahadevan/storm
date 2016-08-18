package org.apache.storm.streams.processors;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.Aggregator;

public class UpdateStateByKeyProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> implements StatefulProcessor<K, R> {
    private final Aggregator<V, R> aggregator;
    private KeyValueState<K, R> keyValueState;

    public UpdateStateByKeyProcessor(Aggregator<V, R> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    protected void execute(Pair<K, V> input) {
        K key = input.getFirst();
        V val = input.getSecond();
        R agg = keyValueState.get(key);
        if (agg == null) {
            agg = aggregator.init();
        }
        R newAgg = aggregator.apply(val, agg);
        keyValueState.put(key, newAgg);
        context.forward(new Pair<>(key, newAgg));
    }

    @Override
    public void initState(KeyValueState<K, R> keyValueState) {
        this.keyValueState = keyValueState;
    }
}
