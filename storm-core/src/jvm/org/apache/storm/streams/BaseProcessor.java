package org.apache.storm.streams;

import java.util.HashSet;
import java.util.Set;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

abstract class BaseProcessor<T> implements Processor<T> {
    protected ProcessorContext context;
    private Set<ProcessorNode> windowedParents;
    private Set<ProcessorNode> punctuationState = new HashSet<>();

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        windowedParents = new HashSet<>(context.getProcessorNode().getWindowedParents().values());
    }

    protected void finish() {
    }

    @Override
    public void punctuate(ProcessorContext parentCtx) {
        if (parentCtx == null || shouldPunctuate(parentCtx.getProcessorNode())) {
            finish();
            context.forward(PUNCTUATION);
            punctuationState.clear();
        }
    }

    final <T> void mayBeForwardAggUpdate(T result) {
        if (!context.isWindowed()) {
            context.forward(result);
        }
    }

    private boolean shouldPunctuate(ProcessorNode parent) {
        punctuationState.add(parent);
        return punctuationState.equals(windowedParents);
    }
}
