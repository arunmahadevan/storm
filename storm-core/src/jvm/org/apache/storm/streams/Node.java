package org.apache.storm.streams;

import org.apache.storm.generated.StreamInfo;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.Map;

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

    protected static Fields getDefaultOutputFields(IComponent bolt, String streamId) {
        OutputFieldsGetter getter = new OutputFieldsGetter();
        bolt.declareOutputFields(getter);
        Map<String, StreamInfo> fieldsDeclaration = getter.getFieldsDeclaration();
        if (fieldsDeclaration != null && fieldsDeclaration.containsKey(streamId)) {
            return new Fields(fieldsDeclaration.get(streamId).get_output_fields());
        }
        return new Fields();
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
