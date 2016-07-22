package org.apache.storm.streams;

import java.util.List;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

class ForwardingProcessorContext implements ProcessorContext {
    private final List<ProcessorNode> children;
    private final boolean windowed;

    public ForwardingProcessorContext(List<ProcessorNode> children) {
        this(children, false);
    }

    public ForwardingProcessorContext(List<ProcessorNode> children, boolean windowed) {
        this.children = children;
        this.windowed = windowed;
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
            processor.punctuate();
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
        return windowed;
    }
}
