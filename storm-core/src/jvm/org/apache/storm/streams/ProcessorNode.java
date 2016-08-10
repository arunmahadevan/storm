package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.util.Set;

class ProcessorNode extends Node {
    private final Processor<?> processor;
    private final boolean isBatch;
    private boolean windowed = false;

    // Windowed parent streams
    private Set<String> windowedParentStreams;

    public ProcessorNode(Processor<?> processor, String outputStream, Fields outputFields) {
        super(outputStream, outputFields);
        this.isBatch = processor instanceof BatchProcessor;
        this.processor = processor;
    }

    public void initProcessorContext(ProcessorContext context) {
        processor.init(context);
    }

    public Processor<?> getProcessor() {
        return processor;
    }

    public boolean isWindowed() {
        return windowed;
    }

    public boolean isBatch() {
        return isBatch;
    }

    public void setWindowed(boolean windowed) {
        this.windowed = windowed;
    }

    Set<String> getWindowedParentStreams() {
        return windowedParentStreams;
    }

    public void setWindowedParentStreams(Set<String> windowedParentStreams) {
        this.windowedParentStreams = windowedParentStreams;
    }

    @Override
    public String toString() {
        return "ProcessorNode{" +
                "processor=" + processor +
                ", windowed=" + windowed +
                ", windowedParentStreams=" + windowedParentStreams +
                "} " + super.toString();
    }
}
