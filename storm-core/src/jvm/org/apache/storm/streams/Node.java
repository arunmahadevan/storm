package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.io.Serializable;

public interface Node extends Serializable {
    Fields getOutputFields();
}
