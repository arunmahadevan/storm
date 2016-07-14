package org.apache.storm.streams;

import org.jgrapht.EdgeFactory;

import java.io.Serializable;

class StreamsEdgeFactory implements EdgeFactory<Node, Edge>, Serializable {
    @Override
    public Edge createEdge(Node sourceVertex, Node targetVertex) {
        return new Edge(sourceVertex, targetVertex);
    }
}
