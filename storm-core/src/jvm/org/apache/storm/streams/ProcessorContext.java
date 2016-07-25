package org.apache.storm.streams;

import java.io.Serializable;

public interface ProcessorContext extends Serializable {
    <T> void forward(T input);

    boolean isWindowed();

    ProcessorNode getProcessorNode();
}
