package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

class EmittingProcessorContext implements ProcessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(EmittingProcessorContext.class);

    private final ProcessorNode processorNode;
    private final String outputStreamId;
    private final OutputCollector collector;
    private final Fields outputFields;
    private final boolean windowed;
    private final Values punctuation;
    private Tuple anchor;

    EmittingProcessorContext(ProcessorNode processorNode, OutputCollector collector) {
        this.processorNode = processorNode;
        this.outputStreamId = processorNode.getOutputStream();
        this.outputFields = processorNode.getOutputFields();
        this.windowed = processorNode.isWindowed();
        this.collector = collector;
        this.punctuation = createPunctuation();
    }

    // TODO: for event ts, create punctuation with event/watermark ts
    private Values createPunctuation() {
        Values values = new Values();
        for (int i = 0; i < outputFields.size(); i++) {
            values.add(PUNCTUATION);
        }
        return values;
    }

    // TODO: for event ts emit with ts
    @Override
    public <T> void forward(T input) {
        if (input instanceof Pair) {
            Pair<?, ?> value = (Pair<?, ?>) input;
            emit(new Values(value.getFirst(), value.getSecond()));
        } else if (PUNCTUATION.equals(input)) {
            emit(punctuation);
        } else {
            emit(new Values(input));
        }
    }

    private void emit(Values values) {
        if (anchor != null) {
            LOG.debug("Emit, outputStreamId: {}, anchor: {}, values: {}", outputStreamId, anchor, values);
            collector.emit(outputStreamId, anchor, values);
        } else {
            LOG.debug("Emit un-anchored, outputStreamId: {}, values: {}", outputStreamId, values);
            // for windowed bolt, windowed output collector will do the anchoring
            collector.emit(outputStreamId, values);
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
