package org.apache.storm.streams;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

abstract class Node implements Serializable {
    protected final Set<String> outputStreams = new HashSet<>();
    protected final Fields outputFields;
    protected String componentId;
    protected int parallelism;

    // the parent streams that this node subscribes to
    private final Multimap<Node, String> parentStreams = ArrayListMultimap.create();

    Node(Set<String> outputStreams, Fields outputFields, String componentId, int parallelism) {
        this.outputStreams.addAll(outputStreams);
        this.outputFields = outputFields;
        this.componentId = componentId;
        this.parallelism = parallelism;
    }

    Node(String outputStream, Fields outputFields, String componentId, int parallelism) {
        this(Collections.singleton(outputStream), outputFields, componentId, parallelism);
    }

    Node(String outputStream, Fields outputFields, String componentId) {
        this(outputStream, outputFields, componentId, 1);
    }

    Node(String outputStream, Fields outputFields) {
        this(outputStream, outputFields, null);
    }

    public void addParentStream(Node parent, String streamId) {
        parentStreams.put(parent, streamId);
    }

    public Fields getOutputFields() {
        return outputFields;
    }

    public Set<String> getOutputStreams() {
        return outputStreams;
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

    Collection<String> getParentStreams(Node parent) {
        return parentStreams.get(parent);
    }

    String addOutputStream(String streamId) {
        outputStreams.add(streamId);
        return streamId;
    }

    @Override
    public String toString() {
        return "Node{" +
                "outputStreams='" + outputStreams + '\'' +
                ", outputFields=" + outputFields +
                ", componentId='" + componentId + '\'' +
                ", parallelism=" + parallelism +
                '}';
    }
}
