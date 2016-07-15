package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

class EmittingProcessorContext implements ProcessorContext {
    private String streamId;
    private final OutputCollector collector;
    private Tuple anchor;

    public EmittingProcessorContext(String streamId, OutputCollector collector) {
        this.streamId = streamId;
        this.collector = collector;
    }

    @Override
    public <T> void forward(T input) {
        if (anchor != null) {
            collector.emit(streamId, anchor, new Values(input));
        } else {
            throw new UnsupportedOperationException("Emitting without an anchor");
        }
    }

    void setAnchor(Tuple anchor) {
        this.anchor = anchor;
    }
}
