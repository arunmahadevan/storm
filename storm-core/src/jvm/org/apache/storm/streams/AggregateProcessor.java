package org.apache.storm.streams;

public class AggregateProcessor<T> implements Processor<T> {
    @Override
    public void execute(T input, ProcessorContext context) {

    }

    void finish(ProcessorContext context) {

    }
}
