package org.apache.storm.streams.processors;

import org.apache.storm.streams.ProcessorNode;

import java.io.Serializable;

public interface ProcessorContext extends Serializable {
    <T> void forward(T input);

    <T> void forward(T input, String stream);

    boolean isWindowed();

    ProcessorNode getProcessorNode();
}
