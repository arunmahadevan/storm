package org.apache.storm.streams;

public class Edge {
    Node source;
    Node target;
    Edge(Node source, Node target) {
        this.source = source;
        this.target = target;
    }
}
