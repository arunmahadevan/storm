package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.util.Map;
import java.util.Set;

class ProcessorNode implements Node {
    private final String streamId;
    private final Processor<?> processor;
    private final Fields outputFields;
    private String componentId;
    private boolean windowed = false;

    // Windowed parent processors
    private Map<String, ProcessorNode> windowedParents;

    public ProcessorNode(String streamId, Processor<?> processor, Fields outputFields) {
        this.streamId = streamId;
        this.processor = processor;
        this.outputFields = outputFields;
    }

    public void initProcessorContext(ProcessorContext context) {
        processor.init(context);
    }

    public Processor<?> getProcessor() {
        return processor;
    }

    // TODO: if multiple output stream should be supported
    @Override
    public Fields getOutputFields() {
        return outputFields;
    }

    @Override
    public String getOutputStream() {
        return streamId;
    }

    public String getStreamId() {
        return streamId;
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public boolean isWindowed() {
        return windowed;
    }

    public void setWindowed(boolean windowed) {
        this.windowed = windowed;
    }

    void setWindowedParents(Map<String, ProcessorNode> windowedParents) {
        this.windowedParents = windowedParents;
    }

    Map<String, ProcessorNode> getWindowedParents() {
        return windowedParents;
    }

    Set<String> getWindowedParentStreams() {
        return windowedParents.keySet();
    }
}
