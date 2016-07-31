package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

class PartitionNode extends Node {
    private final GroupingInfo groupingInfo;

    PartitionNode(String outputStream, Fields outputFields, GroupingInfo groupingInfo) {
        super(outputStream, outputFields);
        this.groupingInfo = groupingInfo;
    }

    PartitionNode(String outputStream, Fields outputFields) {
        this(outputStream, outputFields, GroupingInfo.shuffle());
    }

    GroupingInfo getGroupingInfo() {
        return groupingInfo;
    }

}
