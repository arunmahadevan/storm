package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.Set;

public interface Node extends Serializable {
    Fields getOutputFields();

    // TODO: if multiple output stream should be supported
    String getOutputStream();
}
