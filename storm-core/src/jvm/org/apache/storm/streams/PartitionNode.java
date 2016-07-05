package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

class PartitionNode implements Node {

    @Override
    public Fields getOutputFields() {
        return null;
    }
}
