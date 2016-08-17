package org.apache.storm.streams.processors;

import com.google.common.collect.Multimap;
import org.apache.storm.streams.ProcessorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

public class ForwardingProcessorContext implements ProcessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardingProcessorContext.class);

    private final ProcessorNode processorNode;
    private final Multimap<String, ProcessorNode> streamToChildren;
    private final Set<String> streams;

    public ForwardingProcessorContext(ProcessorNode processorNode, Multimap<String, ProcessorNode> streamToChildren) {
        this.processorNode = processorNode;
        this.streamToChildren = streamToChildren;
        this.streams = streamToChildren.keySet();
    }

    @Override
    public <T> void forward(T input) {
        if (PUNCTUATION.equals(input)) {
            for (String stream : streams) {
                finish(stream);
            }
        } else {
            for (String stream : streams) {
                execute(input, stream);
            }
        }
    }

    @Override
    public <T> void forward(T input, String stream) {
        if (PUNCTUATION.equals(input)) {
            finish(stream);
        } else {
            execute(input, stream);
        }
    }

    private <T> void finish(String stream) {
        for (ProcessorNode node : streamToChildren.get(stream)) {
            LOG.debug("Punctuating processor: {}", node);
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.punctuate(stream);
        }
    }

    private <T> void execute(T input, String stream) {
        for (ProcessorNode node : streamToChildren.get(stream)) {
            LOG.debug("Forward input: {} to processor node: {}", input, node);
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.execute(input, stream);
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
