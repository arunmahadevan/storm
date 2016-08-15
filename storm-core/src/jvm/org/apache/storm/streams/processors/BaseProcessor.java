package org.apache.storm.streams.processors;

import java.util.HashSet;
import java.util.Set;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

abstract class BaseProcessor<T> implements Processor<T> {
    protected ProcessorContext context;
    private Set<String> windowedParentStreams;
    private Set<String> punctuationState = new HashSet<>();

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        windowedParentStreams = new HashSet<>(context.getProcessorNode().getWindowedParentStreams());
    }

    protected void execute(T input) {
    }

    @Override
    public void execute(T input, String streamId) {
        execute(input);
    }

    @Override
    public void punctuate(String stream) {
        if (stream == null || shouldPunctuate(stream)) {
            if (this instanceof BatchProcessor) {
                ((BatchProcessor) this).finish();
            }
            context.forward(PUNCTUATION);
            punctuationState.clear();
        }
    }

    final <T> void mayBeForwardAggUpdate(T result) {
        if (!context.isWindowed()) {
            context.forward(result);
        }
    }

    private boolean shouldPunctuate(String parentStream) {
        punctuationState.add(parentStream);
        return punctuationState.equals(windowedParentStreams);
    }
}
