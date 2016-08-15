package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Reducer;

public class ReduceProcessor<T> extends BaseProcessor<T> implements BatchProcessor {
    private final Reducer<T> reducer;
    private T agg;

    public ReduceProcessor(Reducer<T> reducer) {
        this.reducer = reducer;
    }

    @Override
    public void execute(T input) {
        if (agg == null) {
            agg = input;
        } else {
            agg = reducer.apply(agg, input);
        }
        mayBeForwardAggUpdate(agg);
    }

    @Override
    public void finish() {
        context.forward(agg);
        agg = null;
    }
}
