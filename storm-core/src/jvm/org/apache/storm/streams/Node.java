package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.io.Serializable;

abstract class Node implements Serializable {
    protected final String outputStream;
    protected final Fields outputFields;
    protected String componentId;
    protected int parallelism;

    Node(String outputStream, Fields outputFields, String componentId, int parallelism) {
        this.outputStream = outputStream;
        this.outputFields = outputFields;
        this.componentId = componentId;
        this.parallelism = parallelism;
    }


    Node(String outputStream, Fields outputFields, String componentId) {
        this(outputStream, outputFields, componentId, 1);
    }

    Node(String outputStream, Fields outputFields) {
        this(outputStream, outputFields, null);
    }

    public Fields getOutputFields() {
        return outputFields;
    }

    public String getOutputStream() {
        return outputStream;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public String toString() {
        return "Node{" +
                "outputStream='" + outputStream + '\'' +
                ", outputFields=" + outputFields +
                ", componentId='" + componentId + '\'' +
                ", parallelism=" + parallelism +
                '}';
    }
}
