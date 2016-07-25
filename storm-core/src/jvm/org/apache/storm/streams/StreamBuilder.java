package org.apache.storm.streams;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Set<ProcessorNode> curGroup = new HashSet<>();
        Map<Node, GroupingInfo> nodeGroupingInfo = new HashMap<>();
        WindowNode windowNode = null;
        while (iterator.hasNext()) {
            Node node = iterator.next();
            if (node instanceof SpoutNode) {
                addSpout(topologyBuilder, (SpoutNode) node);
            } else if (node instanceof ProcessorNode) {
                ProcessorNode processorNode = (ProcessorNode) node;
                curGroup.add(processorNode);
                processorNode.setWindowed(isWindowed(processorNode));
            } else if (node instanceof PartitionNode) {
                PartitionNode partitionNode = (PartitionNode) node;
                for (Node parent : parentNodes(partitionNode)) {
                    nodeGroupingInfo.put(parent, partitionNode.getGroupingInfo());
                }
                if (processCurGroup(topologyBuilder, curGroup, nodeGroupingInfo, windowNode)) {
                    windowNode = null;
                }
            } else if (node instanceof WindowNode) {
                processCurGroup(topologyBuilder, curGroup, nodeGroupingInfo, windowNode);
                windowNode = (WindowNode) node;
            }
        }
        processCurGroup(topologyBuilder, curGroup, nodeGroupingInfo, windowNode);
        return topologyBuilder.createTopology();
    }

    private List<Node> parentNodes(Node curNode) {
        List<Node> nodes = new ArrayList<>();
        for (Node parent : StreamUtil.<Node>getParents(graph, curNode)) {
            if (parent instanceof ProcessorNode || parent instanceof SpoutNode) {
                nodes.add(parent);
            } else {
                nodes.addAll(parentNodes(parent));
            }
        }
        return nodes;
    }

    private boolean processCurGroup(TopologyBuilder topologyBuilder,
                                    Set<ProcessorNode> curGroup,
                                    Map<Node, GroupingInfo> nodeGroupingInfo,
                                    WindowNode windowNode) {
        if (curGroup.isEmpty()) {
            return false;
        } else if (windowNode == null) {
            addBolt(topologyBuilder, curGroup, nodeGroupingInfo);
        } else {
            addWindowedBolt(topologyBuilder, curGroup, nodeGroupingInfo, windowNode);
        }
        curGroup.clear();
        return true;
    }

    private void addSpout(TopologyBuilder topologyBuilder, SpoutNode spoutNode) {
        String spoutId = UniqueIdGen.getInstance().getUniqueSpoutId();
        topologyBuilder.setSpout(spoutId, spoutNode.getSpout());
        spoutNode.setComponentId(spoutId);
    }

    private void addWindowedBolt(TopologyBuilder topologyBuilder,
                                 Set<ProcessorNode> curGroup,
                                 Map<Node, GroupingInfo> nodeGroupingInfo,
                                 WindowNode windowNode) {
        WindowedProcessorBolt bolt = new WindowedProcessorBolt(graph, curGroup, windowNode);
        String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
        for (ProcessorNode processorNode : curGroup) {
            processorNode.setComponentId(boltId);
            processorNode.setWindowedParents(getWindowedParents(processorNode));
        }
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt);
        List<ProcessorNode> initialProcessors = initialProcessors(curGroup);
        Multimap<String, ProcessorNode> streamToInitialProcessors =
                wireBolt(boltDeclarer, initialProcessors, nodeGroupingInfo);
        bolt.setStreamToInitialProcessors(streamToInitialProcessors);
    }

    private void addBolt(TopologyBuilder topologyBuilder,
                         Set<ProcessorNode> curGroup,
                         Map<Node, GroupingInfo> nodeGroupingInfo) {
        ProcessorBolt bolt = new ProcessorBolt(graph, curGroup);
        String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
        for (ProcessorNode processorNode : curGroup) {
            processorNode.setComponentId(boltId);
            processorNode.setWindowedParents(getWindowedParents(processorNode));
        }
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt);
        List<ProcessorNode> initialProcessors = initialProcessors(curGroup);
        Multimap<String, ProcessorNode> streamToInitialProcessors =
                wireBolt(boltDeclarer, initialProcessors, nodeGroupingInfo);
        bolt.setStreamToInitialProcessors(streamToInitialProcessors);
    }

    private Map<String, ProcessorNode> getWindowedParents(ProcessorNode processorNode) {
        Map<String, ProcessorNode> res = new HashMap<>();
        for (Node parent : parentNodes(processorNode)) {
            if (parent instanceof ProcessorNode) {
                ProcessorNode pn = (ProcessorNode) parent;
                if (pn.isWindowed()) {
                    res.put(pn.getOutputStream(), pn);
                }
            }
        }
        return res;
    }

    private Multimap<String, ProcessorNode> wireBolt(BoltDeclarer boltDeclarer,
                                                     List<ProcessorNode> initialProcessors,
                                                     Map<Node, GroupingInfo> nodeGroupingInfo) {
        Multimap<String, ProcessorNode> streamToInitialProcessor = ArrayListMultimap.create();
        for (ProcessorNode curNode : initialProcessors) {
            for (Node parent : parentNodes(curNode)) {
                GroupingInfo groupingInfo = nodeGroupingInfo.get(parent);
                if (groupingInfo == null) {
                    boltDeclarer.shuffleGrouping(parent.getComponentId(), parent.getOutputStream());
                } else if (groupingInfo.getGrouping() == GroupingInfo.Grouping.GLOBAL) {
                    boltDeclarer.globalGrouping(parent.getComponentId(), parent.getOutputStream());
                } else if (groupingInfo.getGrouping() == GroupingInfo.Grouping.FIELDS) {
                    boltDeclarer.fieldsGrouping(parent.getComponentId(), parent.getOutputStream(),
                            groupingInfo.getFields());
                }
                streamToInitialProcessor.put(parent.getOutputStream(), curNode);
            }
        }
        return streamToInitialProcessor;
    }

    private List<ProcessorNode> initialProcessors(Set<ProcessorNode> curGroup) {
        List<ProcessorNode> nodes = new ArrayList<>();
        DirectedSubgraph<Node, Edge> subgraph = new DirectedSubgraph<>(graph, new HashSet<Node>(curGroup), null);
        for (Node pn : subgraph.vertexSet()) {
            List<ProcessorNode> parents = StreamUtil.getParents(subgraph, pn);
            if (parents.isEmpty()) {
                nodes.add((ProcessorNode) pn);
            }
        }
        return nodes;
    }

    private boolean isWindowed(Node curNode) {
        for (Node parent : StreamUtil.<Node>getParents(graph, curNode)) {
            if (parent instanceof WindowNode) {
                return true;
            } else if (parent instanceof ProcessorNode) {
                ProcessorNode p = ((ProcessorNode) parent);
                if (p.isWindowed()) {
                    return true;
                }
            } else if (parent instanceof PartitionNode) {
                return isWindowed(parent);
            } else {
                return false;
            }
        }
        return false;
    }
}
