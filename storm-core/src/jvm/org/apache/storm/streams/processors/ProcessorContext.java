package org.apache.storm.streams.processors;

import org.apache.storm.streams.ProcessorNode;

import java.io.Serializable;

public interface ProcessorContext extends Serializable {
    <T> void forward(T input);

    boolean isWindowed();

    ProcessorNode getProcessorNode();
}
