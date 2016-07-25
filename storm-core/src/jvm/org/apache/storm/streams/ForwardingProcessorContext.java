package org.apache.storm.streams;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

class ForwardingProcessorContext implements ProcessorContext {
    private final ProcessorNode processorNode;
    private final List<ProcessorNode> children;

    public ForwardingProcessorContext(ProcessorNode processorNode, List<ProcessorNode> children) {
        this.processorNode = processorNode;
        this.children = children;
    }

    @Override
    public <T> void forward(T input) {
        // TODO: received from all parents
        if (PUNCTUATION.equals(input)) {
            finish();
        } else {
            execute(input);
        }
    }

    private <T> void finish() {
        for (ProcessorNode node : children) {
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.punctuate(this);
        }
    }

    private <T> void execute(T input) {
        for (ProcessorNode node : children) {
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.execute(input);
        }
    }

    @Override
    public boolean isWindowed() {
        return processorNode.isWindowed();
    }

    @Override
    public ProcessorNode getProcessorNode() {
        return processorNode;
    }
}
