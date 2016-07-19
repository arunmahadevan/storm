package org.apache.storm.streams;

class ReduceProcessor<T> extends BaseProcessor<T> {
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
        // TODO: do this only if the stream is not windowed
        context.forward(agg);
    }

    // TODO: should be invoked from a windowed bolt
    void finish() {
        context.forward(agg);
        agg = null;
    }
}
