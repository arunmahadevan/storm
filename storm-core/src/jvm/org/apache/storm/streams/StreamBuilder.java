package org.apache.storm.streams;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

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

    Node addNode(Stream parent, Node newNode) {
        graph.addVertex(newNode);
        graph.addEdge(parent.getNode(), newNode);
        return newNode;
    }

    public Stream<Tuple> newStream(IRichSpout spout) {
        SpoutNode spoutNode = new SpoutNode(spout);
        graph.addVertex(spoutNode);
        return new Stream<>(this, spoutNode);
    }

    public <T> Stream<T> newStream(IRichSpout spout, TupleValueMapper<T> valueMapper) {
        Stream<Tuple> spoutStream = newStream(spout);
        Node mapNode = addNode(spoutStream, new ProcessorNode(UniqueIdGen.getInstance().getUniqueStreamId(),
                new MapProcessor<>(valueMapper), new Fields("value")));
        return new Stream<>(this, mapNode);
    }

    public StormTopology build() {
        TopologicalOrderIterator<Node, Edge> iterator = new TopologicalOrderIterator<>(graph);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        List<Processor> processors = new ArrayList<>();
        while (iterator.hasNext()) {
            Node node = iterator.next();
            if (node instanceof SpoutNode) {
                SpoutNode spoutNode = (SpoutNode) node;
                String spoutId = UniqueIdGen.getInstance().getUniqueSpoutId();
                topologyBuilder.setSpout(spoutId, spoutNode.getSpout());
                spoutNode.setComponentId(spoutId);
            } else if (node instanceof ProcessorNode) {
                ProcessorNode processorNode = (ProcessorNode) node;
                String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
                IRichBolt bolt = new ProcessorBolt(graph, Collections.singleton(processorNode));
                BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt);
                wireBolt(boltDeclarer, processorNode);
                // TODO: put windowing operations into Windowed bolt
                processorNode.setComponentId(boltId);
            }
        }
//        if (!processors.isEmpty()) {
//            topologyBuilder.setBolt("TODO", new ProcessorBolt(processors)).shuffleGrouping("TODO");
//        }
        return topologyBuilder.createTopology();
    }

    private void wireBolt(BoltDeclarer boltDeclarer, Node curNode) {
        wireBolt(boltDeclarer, curNode, GroupingInfo.shuffle());
    }

    private void wireBolt(BoltDeclarer boltDeclarer, Node curNode, GroupingInfo groupingInfo) {
        for (Node parent : StreamUtil.<Node>getParents(graph, curNode)) {
            if (parent instanceof PartitionNode) {
                PartitionNode partitionNode = (PartitionNode) parent;
                wireBolt(boltDeclarer, partitionNode, partitionNode.getGroupingInfo());
            } else { // ProcessorNode or SpoutNode
                if (groupingInfo.getGrouping() == GroupingInfo.Grouping.FIELDS) {
                    boltDeclarer.fieldsGrouping(parent.getComponentId(), parent.getOutputStream(),
                            groupingInfo.getFields());
                } else if (groupingInfo.getGrouping() == GroupingInfo.Grouping.GLOBAL) {
                    boltDeclarer.globalGrouping(parent.getComponentId(), parent.getOutputStream());
                } else {
                    boltDeclarer.shuffleGrouping(parent.getComponentId(), parent.getOutputStream());
                }
            }
        }
    }
}
