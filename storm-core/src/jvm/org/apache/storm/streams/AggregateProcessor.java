package org.apache.storm.streams;

class AggregateProcessor<T, R> extends BaseProcessor<T> {
    private final Aggregator<T, R> aggregator;
    private R state = null;

    AggregateProcessor(Aggregator<T, R> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public void execute(T input) {
        if (state == null) {
            state = aggregator.init();
        }
        state = aggregator.apply(input, state != null ? state : aggregator.init());
        // TODO: do this only if the stream is not windowed
        context.forward(state);
    }

    // TODO: should be invoked from a windowed bolt
    void finish() {
        context.forward(state);
        state = null;
    }
}
