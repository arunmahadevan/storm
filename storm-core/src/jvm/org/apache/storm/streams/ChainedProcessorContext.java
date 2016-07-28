package org.apache.storm.streams;

import java.util.Arrays;
import java.util.List;

class ChainedProcessorContext implements ProcessorContext {
    private final ProcessorNode processorNode;
    private final List<ProcessorContext> contexts;

    ChainedProcessorContext(ProcessorNode processorNode, List<ProcessorContext> contexts) {
        this.processorNode = processorNode;
        this.contexts = contexts;
    }

    ChainedProcessorContext(ProcessorNode processorNode, ProcessorContext... contexts) {
        this(processorNode, Arrays.asList(contexts));
    }

    @Override
    public <T> void forward(T input) {
        for (ProcessorContext context : contexts) {
            context.forward(input);
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
