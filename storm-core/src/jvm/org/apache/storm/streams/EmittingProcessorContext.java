package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;

class EmittingProcessorContext implements ProcessorContext {
    private final OutputCollector collector;

    public EmittingProcessorContext(OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public <T> void forward(T input) {
        // TODO: anchoring
        collector.emit(new Values(input));
    }
}
