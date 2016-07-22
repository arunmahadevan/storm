package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

class EmittingProcessorContext implements ProcessorContext {
    private final String streamId;
    private final OutputCollector collector;
    private final Fields outputFields;
    private final boolean windowed;
    private final Values punctuation;
    private Tuple anchor;

    public EmittingProcessorContext(String streamId, OutputCollector collector, Fields outputFields) {
        this(streamId, collector, outputFields, false);
    }

    public EmittingProcessorContext(String streamId, OutputCollector collector, Fields outputFields, boolean windowed) {
        this.streamId = streamId;
        this.collector = collector;
        this.outputFields = outputFields;
        this.windowed = windowed;
        this.punctuation = createPunctuation();
    }

    private Values createPunctuation() {
        Values values = new Values();
        for (int i = 0; i < outputFields.size(); i++) {
            values.add(PUNCTUATION);
        }
        return values;
    }

    @Override
    public <T> void forward(T input) {
        if (anchor == null) {
            throw new UnsupportedOperationException("Emitting without an anchor");
        }

        if (input instanceof Pair) {
            Pair<?, ?> value = (Pair<?, ?>) input;
            collector.emit(streamId, anchor, new Values(value.getFirst(), value.getSecond()));
        } else if (PUNCTUATION.equals(input)) {
            collector.emit(streamId, anchor, punctuation);
        } else {
            collector.emit(streamId, anchor, new Values(input));
        }
    }

    void setAnchor(Tuple anchor) {
        this.anchor = anchor;
    }

    @Override
    public boolean isWindowed() {
        return windowed;
    }
}
