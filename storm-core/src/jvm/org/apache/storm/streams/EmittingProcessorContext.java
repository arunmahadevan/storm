package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

class EmittingProcessorContext implements ProcessorContext {
    private final ProcessorNode processorNode;
    private final String outputStreamId;
    private final OutputCollector collector;
    private final Fields outputFields;
    private final boolean windowed;
    private final Values punctuation;
    private Tuple anchor;

    public EmittingProcessorContext(ProcessorNode processorNode, OutputCollector collector) {
        this.processorNode = processorNode;
        this.outputStreamId = processorNode.getStreamId();
        this.outputFields = processorNode.getOutputFields();
        this.windowed = processorNode.isWindowed();
        this.collector = collector;
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
            collector.emit(outputStreamId, anchor, new Values(value.getFirst(), value.getSecond()));
        } else if (PUNCTUATION.equals(input)) {
            collector.emit(outputStreamId, anchor, punctuation);
        } else {
            collector.emit(outputStreamId, anchor, new Values(input));
        }
    }

    void setAnchor(Tuple anchor) {
        this.anchor = anchor;
    }

    @Override
    public boolean isWindowed() {
        return windowed;
    }

    @Override
    public ProcessorNode getProcessorNode() {
        return processorNode;
    }
}
