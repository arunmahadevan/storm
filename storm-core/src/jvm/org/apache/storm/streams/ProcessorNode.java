package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.util.Collections;
import java.util.Set;

class ProcessorNode implements Node {
    private final Processor<?> processor;
    private final Fields outputFields;
    private final String streamId;

    public ProcessorNode(String streamId, Processor<?> processor, Fields outputFields) {
        this.processor = processor;
        this.outputFields = outputFields;
        this.streamId = streamId;
    }

    public void initProcessorContext(ProcessorContext context) {
        processor.init(context);
    }

    public Processor<?> getProcessor() {
        return processor;
    }

    @Override
    public Fields getOutputFields() {
        return outputFields;
    }

    @Override
    public String getOutputStream() {
        return streamId;
    }
}
