package org.apache.storm.streams.processors;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.ProcessorNode;
import org.apache.storm.streams.RefCountedTuple;
import org.apache.storm.streams.StreamUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

public class EmittingProcessorContext implements ProcessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(EmittingProcessorContext.class);

    private final ProcessorNode processorNode;
    private final String outputStreamId;
    private final OutputCollector collector;
    private final Fields outputFields;
    private final boolean windowed;
    private final Values punctuation;
    private final List<RefCountedTuple> anchors = new ArrayList<>();
    private boolean emitPunctuation = true;
    private long eventTimestamp = 0L;
    private String timestampField;

    public EmittingProcessorContext(ProcessorNode processorNode, OutputCollector collector, String outputStreamId) {
        this.processorNode = processorNode;
        this.outputStreamId = outputStreamId;
        this.outputFields = processorNode.getOutputFields();
        this.windowed = processorNode.isWindowed();
        this.collector = collector;
        this.punctuation = createPunctuation();
    }

    @Override
    public <T> void forward(T input) {
        if (input instanceof Pair) {
            Pair<?, ?> value = (Pair<?, ?>) input;
            emit(new Values(value.getFirst(), value.getSecond()));
        } else if (PUNCTUATION.equals(input)) {
            if (emitPunctuation) {
                emit(punctuation);
            } else {
                LOG.debug("Not emitting punctuation since emitPunctuation is false");
            }
            maybeAck();
        } else {
            emit(new Values(input));
        }
    }

    @Override
    public <T> void forward(T input, String stream) {
        if (stream.equals(outputStreamId)) {
            forward(input);
        }
    }

    @Override
    public boolean isWindowed() {
        return windowed;
    }

    @Override
    public ProcessorNode getProcessorNode() {
        return processorNode;
    }

    public boolean isEmitPunctuation() {
        return emitPunctuation;
    }

    public void setEmitPunctuation(boolean emitPunctuation) {
        this.emitPunctuation = emitPunctuation;
    }

    public void setTimestampField(String fieldName) {
        this.timestampField = fieldName;
    }

    public void setAnchor(RefCountedTuple anchor) {
        if (processorNode.isWindowed() && processorNode.isBatch()) {
            anchor.increment();
            anchors.add(anchor);
        } else {
            if (anchors.isEmpty()) {
                anchors.add(anchor);
            } else {
                anchors.set(0, anchor);
            }
             /*
             * track punctuation in non-batch mode so that the
             * punctuation is acked after all the processors have emitted the punctuation downstream.
             */
            if (StreamUtil.isPunctuation(anchor.tuple().getValue(0))) {
                anchor.increment();
            }
        }
    }

    public void setEventTimestamp(long timestamp) {
        this.eventTimestamp = timestamp;
    }

    // TODO: for event ts, create punctuation with event/watermark ts
    private Values createPunctuation() {
        Values values = new Values();
        for (int i = 0; i < outputFields.size(); i++) {
            values.add(PUNCTUATION);
        }
        return values;
    }

    private void maybeAck() {
        if (!anchors.isEmpty()) {
            for (RefCountedTuple anchor : anchors) {
                anchor.decrement();
                if (anchor.shouldAck()) {
                    LOG.info("Acking {} ", anchor);
                    collector.ack(anchor.tuple());
                }
            }
            anchors.clear();
        }
    }

    private Collection<Tuple> tuples(List<RefCountedTuple> anchors) {
        return Collections2.transform(anchors, new Function<RefCountedTuple, Tuple>() {
            @Override
            public Tuple apply(RefCountedTuple input) {
                return input.tuple();
            }
        });
    }

    private void emit(Values values) {
        if (timestampField != null) {
            values.add(eventTimestamp);
        }
        if (!anchors.isEmpty()) {
            LOG.debug("Emit, outputStreamId: {}, anchors: {}, values: {}", outputStreamId, anchors, values);
            collector.emit(outputStreamId, tuples(anchors), values);
        } else {
            // for windowed bolt, windowed output collector will do the anchoring/acking
            LOG.debug("Emit un-anchored, outputStreamId: {}, values: {}", outputStreamId, values);
            collector.emit(outputStreamId, values);
        }
    }
}
