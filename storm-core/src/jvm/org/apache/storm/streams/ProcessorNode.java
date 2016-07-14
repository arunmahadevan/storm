package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

class ProcessorNode implements Node {
    private final Processor<?> processor;
    private final Fields outputFields;

    public ProcessorNode(Processor<?> processor, Fields outputFields) {
        this.processor = processor;
        this.outputFields = outputFields;
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
}
