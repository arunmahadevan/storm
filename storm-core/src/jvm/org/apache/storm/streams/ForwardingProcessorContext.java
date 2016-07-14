package org.apache.storm.streams;

import java.util.List;

class ForwardingProcessorContext implements ProcessorContext {
    private final List<ProcessorNode> children;

    public ForwardingProcessorContext(List<ProcessorNode> children) {
        this.children = children;
    }

    @Override
    public <T> void forward(T input) {
        for (ProcessorNode node : children) {
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.execute(input);
        }
    }
}
