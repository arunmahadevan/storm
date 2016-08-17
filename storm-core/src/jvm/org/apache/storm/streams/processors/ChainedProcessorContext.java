package org.apache.storm.streams.processors;

import org.apache.storm.streams.ProcessorNode;

import java.util.Arrays;
import java.util.List;

public class ChainedProcessorContext implements ProcessorContext {
    private final ProcessorNode processorNode;
    private final List<? extends ProcessorContext> contexts;

    public ChainedProcessorContext(ProcessorNode processorNode, List<? extends ProcessorContext> contexts) {
        this.processorNode = processorNode;
        this.contexts = contexts;
    }

    public ChainedProcessorContext(ProcessorNode processorNode, ProcessorContext... contexts) {
        this(processorNode, Arrays.asList(contexts));
    }

    @Override
    public <T> void forward(T input) {
        for (ProcessorContext context : contexts) {
            context.forward(input);
        }
    }

    @Override
    public <T> void forward(T input, String stream) {
        for (ProcessorContext context : contexts) {
            context.forward(input, stream);
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
