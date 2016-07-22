package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

class WindowNode implements Node {
    private final String streamId;
    private final Fields outputFields;
    private String componentId;

    public static final String PUNCTUATION = "__punctuation";

    public WindowNode(String streamId, Fields outputFields) {
        this.streamId = streamId;
        this.outputFields = outputFields;
    }

    @Override
    public Fields getOutputFields() {
        return outputFields;
    }

    @Override
    public String getOutputStream() {
        return streamId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    @Override
    public String getComponentId() {
        return componentId;
    }
}
