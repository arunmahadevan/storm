package org.apache.storm.streams;

import org.jgrapht.EdgeFactory;

public class StreamsEdgeFactory implements EdgeFactory<Node, Edge> {
    @Override
    public Edge createEdge(Node sourceVertex, Node targetVertex) {
        return new Edge(sourceVertex, targetVertex);
    }
}
