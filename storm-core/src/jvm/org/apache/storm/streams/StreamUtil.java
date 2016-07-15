package org.apache.storm.streams;

import org.jgrapht.DirectedGraph;

import java.util.ArrayList;
import java.util.List;

class StreamUtil {
    static  <T> List<T> getParents(DirectedGraph<Node, Edge> graph, Node node) {
        List<Edge> incoming = new ArrayList<>(graph.incomingEdgesOf(node));
        List<T> ret = new ArrayList<>();
        for (Edge e : incoming) {
            ret.add((T)e.getSource());
        }
        return ret;
    }

    static  <T> List<T> getChildren(DirectedGraph<Node, Edge> graph, Node node) {
        List<Edge> outgoing = new ArrayList<>(graph.outgoingEdgesOf(node));
        List<T> ret = new ArrayList<>();
        for (Edge e : outgoing) {
            ret.add((T)e.getTarget());
        }
        return ret;
    }

}
