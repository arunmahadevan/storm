package org.apache.storm.streams;

import java.io.Serializable;

class Edge implements Serializable {
    private final Node source;
    private final Node target;

    Edge(Node source, Node target) {
        this.source = source;
        this.target = target;
    }

    public Node getSource() {
        return source;
    }

    public Node getTarget() {
        return target;
    }
}
