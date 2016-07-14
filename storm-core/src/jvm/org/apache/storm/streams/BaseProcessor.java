package org.apache.storm.streams;

abstract class BaseProcessor<T> implements Processor<T> {
    protected ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }
}
