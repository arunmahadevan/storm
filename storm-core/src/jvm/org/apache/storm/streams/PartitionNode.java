package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

class PartitionNode implements Node {
    private final String outputStream;
    private final Fields outputFields;
    private final GroupingInfo groupingInfo;

    PartitionNode(String outputStream, Fields outputFields, GroupingInfo groupingInfo) {
        this.outputStream = outputStream;
        this.outputFields = outputFields;
        this.groupingInfo = groupingInfo;
    }

    @Override
    public Fields getOutputFields() {
        return outputFields;
    }

    @Override
    public String getOutputStream() {
        return outputStream;
    }

    public GroupingInfo getGroupingInfo() {
        return groupingInfo;
    }

    @Override
    public String getComponentId() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
