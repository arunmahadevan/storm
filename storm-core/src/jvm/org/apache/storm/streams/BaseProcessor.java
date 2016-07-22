package org.apache.storm.streams;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

abstract class BaseProcessor<T> implements Processor<T> {
    protected ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    protected void finish() {
    }

    @Override
    public void punctuate() {
        finish();
        context.forward(PUNCTUATION);
    }

    final <T> void forwardAggUpdate(T result) {
        if (!context.isWindowed()) {
            context.forward(result);
        }
    }
}
