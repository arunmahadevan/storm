package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.util.Set;

class PartitionNode implements Node {

    @Override
    public Fields getOutputFields() {
        return null;
    }

    @Override
    public String getOutputStream() {
        return null;
    }
}
