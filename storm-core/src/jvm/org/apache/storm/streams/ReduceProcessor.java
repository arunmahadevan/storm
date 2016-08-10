package org.apache.storm.streams;

class ReduceProcessor<T> extends BaseProcessor<T> implements BatchProcessor {
    private final Reducer<T> reducer;
    private T agg;

    ReduceProcessor(Reducer<T> reducer) {
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
