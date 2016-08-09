package org.apache.storm.streams;

import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

class ForwardingProcessorContext implements ProcessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardingProcessorContext.class);

    private final ProcessorNode processorNode;
    private final Multimap<String, ProcessorNode> streamToChildren;

    ForwardingProcessorContext(ProcessorNode processorNode, Multimap<String, ProcessorNode> streamToChildren) {
        this.processorNode = processorNode;
        this.streamToChildren = streamToChildren;
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
        for (Map.Entry<String, ProcessorNode> entry : streamToChildren.entries()) {
            LOG.debug("Punctuating processor: {}", entry.getValue());
            Processor<T> processor = (Processor<T>) entry.getValue().getProcessor();
            processor.punctuate(entry.getKey());
        }
    }

    private <T> void execute(T input) {
        for (Map.Entry<String, ProcessorNode> entry : streamToChildren.entries()) {
            LOG.debug("Forward input: {} to processor node: {}", input, entry.getValue());
            Processor<T> processor = (Processor<T>) entry.getValue().getProcessor();
            processor.execute(input, entry.getKey());
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
