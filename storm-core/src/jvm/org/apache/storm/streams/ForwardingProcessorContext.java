package org.apache.storm.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

class ForwardingProcessorContext implements ProcessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardingProcessorContext.class);

    private final ProcessorNode processorNode;
    private final List<ProcessorNode> children;

    ForwardingProcessorContext(ProcessorNode processorNode, List<ProcessorNode> children) {
        this.processorNode = processorNode;
        this.children = children;
    }

    @Override
    public <T> void forward(T input) {
        if (PUNCTUATION.equals(input)) {
            finish();
        } else {
            execute(input);
        }
    }

    private <T> void finish() {
        for (ProcessorNode node : children) {
            LOG.debug("Punctuating processor: {}", node);
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.punctuate(processorNode.getOutputStream());
        }
    }

    private <T> void execute(T input) {
        for (ProcessorNode node : children) {
            LOG.debug("Forward input: {} to processor node: {}", input, node);
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.execute(input, processorNode.getOutputStream());
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
