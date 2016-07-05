package org.apache.storm.streams;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
TODO:
    1. figure out Ids
    2. group by and aggregate
    3. grouping fields/shuffle etc
 */
public class StreamBuilder {
    private final DefaultDirectedGraph<Node, Edge> graph;

    public StreamBuilder() {
        graph = new DefaultDirectedGraph<>(new StreamsEdgeFactory());
    }

    ProcessorNode addNode(Stream stream, ProcessorNode newNode) {
        graph.addVertex(newNode);
        graph.addEdge(stream.getNode(), newNode);
        return newNode;
    }

    public Stream<Tuple> newStream(IRichSpout spout) {
        SpoutNode spoutNode = new SpoutNode(spout);
        graph.addVertex(spoutNode);
        return new Stream<>(this, spoutNode);
    }

    public <T> Stream<T> newStream(IRichSpout spout, TupleValueMapper<T> valueMapper) {
        Stream<Tuple> spoutStream = newStream(spout);
        ProcessorNode mapNode = addNode(spoutStream, new ProcessorNode(new MapProcessor<>(valueMapper), new Fields("value")));
        return new Stream<>(this, mapNode);
    }

    public StormTopology build() {
        TopologicalOrderIterator<Node, Edge> iterator = new TopologicalOrderIterator<>(graph);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        List<Processor> processors = new ArrayList<>();
        Map<Node, String> nodeToNodeId = new HashMap<>();
        while (iterator.hasNext()) {
            Node node =  iterator.next();
            if (node instanceof SpoutNode) {
                String spoutId = UniqueIdGen.getInstance().getUniqueSpoutId();
                topologyBuilder.setSpout(spoutId, ((SpoutNode) node).getSpout());
                nodeToNodeId.put(node, spoutId);
            } else if (node instanceof ProcessorNode) {
                String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
                IRichBolt bolt = new ProcessorBolt(Collections.singletonList(((ProcessorNode) node)));
                BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt);
                for (Edge edge: graph.incomingEdgesOf(node)) {
                    boltDeclarer.shuffleGrouping(nodeToNodeId.get(edge.source));
                    if (edge.source instanceof SpoutNode) {

                    }
                }
                nodeToNodeId.put(node, boltId);
            } else if (node instanceof PartitionNode) {
                // TODO
            }
        }
//        if (!processors.isEmpty()) {
//            topologyBuilder.setBolt("TODO", new ProcessorBolt(processors)).shuffleGrouping("TODO");
//        }
        return topologyBuilder.createTopology();
    }
}
