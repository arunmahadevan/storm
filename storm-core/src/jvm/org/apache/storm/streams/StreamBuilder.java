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
import org.apache.storm.windowing.Window;
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
        List<ProcessorNode> curGroup = new ArrayList<>();
        Map<Node, GroupingInfo> nodeGroupingInfo = new HashMap<>();
        Map<Node, WindowNode> windowInfo = new HashMap<>();
        while (iterator.hasNext()) {
            Node node = iterator.next();
            if (node instanceof SpoutNode) {
                addSpout(topologyBuilder, (SpoutNode) node);
            } else if (node instanceof ProcessorNode) {
                curGroup.add((ProcessorNode) node);
            } else if (node instanceof PartitionNode) {
                updateNodeGroupingInfo(nodeGroupingInfo, (PartitionNode) node);
                processCurGroup(topologyBuilder, curGroup, nodeGroupingInfo, windowInfo);
            } else if (node instanceof WindowNode) {
                updateWindowInfo(windowInfo, (WindowNode) node);
                processCurGroup(topologyBuilder, curGroup, nodeGroupingInfo, windowInfo);
            }
        }
        processCurGroup(topologyBuilder, curGroup, nodeGroupingInfo, windowInfo);
        return topologyBuilder.createTopology();
    }

    private void updateNodeGroupingInfo(Map<Node, GroupingInfo> nodeGroupingInfo,
                                        PartitionNode partitionNode) {
        for (Node parent : parentNodes(partitionNode)) {
            nodeGroupingInfo.put(parent, partitionNode.getGroupingInfo());
        }
    }

    private void updateWindowInfo(Map<Node, WindowNode> windowInfo,
                                  WindowNode windowNode) {
        for (Node parent : parentNodes(windowNode)) {
            windowInfo.put(parent, windowNode);
        }
    }

    private Set<Node> parentNodes(List<? extends Node> nodes) {
        Set<Node> parents = new HashSet<>();
        for (Node node : nodes) {
            parents.addAll(parentNodes(node));
        }
        return parents;
    }

    private Set<Node> parentNodes(Node curNode) {
        Set<Node> nodes = new HashSet<>();
        for (Node parent : StreamUtil.<Node>getParents(graph, curNode)) {
            if (parent instanceof ProcessorNode || parent instanceof SpoutNode) {
                nodes.add(parent);
            } else {
                nodes.addAll(parentNodes(parent));
            }
        }
        return nodes;
    }

    Node parentNode(Node curNode) {
        Set<Node> parentNode = parentNodes(curNode);
        if (parentNode.size() > 1) {
            throw new IllegalArgumentException("Node " + curNode + " has more than one parent node.");
        } else if (parentNode.size() == 0) {
            throw new IllegalArgumentException("Node " + curNode + " has no parent.");
        }
        return parentNode.iterator().next();
    }

    private void processCurGroup(TopologyBuilder topologyBuilder,
                                 List<ProcessorNode> curGroup,
                                 Map<Node, GroupingInfo> nodeGroupingInfo,
                                 Map<Node, WindowNode> windowInfo) {
        if (curGroup.isEmpty()) {
            return;
        }

        String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
        for (ProcessorNode processorNode : curGroup) {
            processorNode.setComponentId(boltId);
            processorNode.setWindowed(isWindowed(processorNode));
            processorNode.setWindowedParentStreams(getWindowedParentStreams(processorNode));
        }
        List<ProcessorNode> initialProcessors = initialProcessors(curGroup);
        Set<WindowNode> windowNodes = getWindowNodes(initialProcessors, windowInfo);
        if (windowNodes.isEmpty()) {
            addBolt(topologyBuilder, boltId, curGroup, initialProcessors, nodeGroupingInfo);
        } else if (windowNodes.size() == 1) {
            WindowNode windowNode = windowNodes.iterator().next();
            addWindowedBolt(topologyBuilder, boltId, curGroup, initialProcessors, nodeGroupingInfo, windowNode);
        } else {
            throw new IllegalStateException("More than one window config for current group " + curGroup);
        }
        curGroup.clear();
    }

    private Set<WindowNode> getWindowNodes(List<ProcessorNode> initialProcessors,
                                           Map<Node, WindowNode> windowInfo) {
        Set<WindowNode> res = new HashSet<>();
        for (Node node : parentNodes(initialProcessors)) {
            if (windowInfo.containsKey(node)) {
                res.add(windowInfo.get(node));
            }
        }
        return res;
    }

    private void addSpout(TopologyBuilder topologyBuilder, SpoutNode spoutNode) {
        String spoutId = UniqueIdGen.getInstance().getUniqueSpoutId();
        topologyBuilder.setSpout(spoutId, spoutNode.getSpout());
        spoutNode.setComponentId(spoutId);
    }

    private void addWindowedBolt(TopologyBuilder topologyBuilder,
                                 String boltId,
                                 List<ProcessorNode> curGroup,
                                 List<ProcessorNode> initialProcessors,
                                 Map<Node, GroupingInfo> nodeGroupingInfo,
                                 WindowNode windowNode) {
        WindowedProcessorBolt bolt = new WindowedProcessorBolt(graph, curGroup, windowNode);
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt);
        bolt.setStreamToInitialProcessors(wireBolt(boltDeclarer, curGroup, initialProcessors, nodeGroupingInfo));
    }

    private void addBolt(TopologyBuilder topologyBuilder,
                         String boltId,
                         List<ProcessorNode> curGroup,
                         List<ProcessorNode> initialProcessors,
                         Map<Node, GroupingInfo> nodeGroupingInfo) {
        ProcessorBolt bolt = new ProcessorBolt(graph, curGroup);
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt);
        bolt.setStreamToInitialProcessors(wireBolt(boltDeclarer, curGroup, initialProcessors, nodeGroupingInfo));
    }

    private Set<String> getWindowedParentStreams(ProcessorNode processorNode) {
        Set<String> res = new HashSet<>();
        for (Node parent : parentNodes(processorNode)) {
            if (parent instanceof ProcessorNode) {
                ProcessorNode pn = (ProcessorNode) parent;
                if (pn.isWindowed()) {
                    res.add(pn.getOutputStream());
                }
            }
        }
        return res;
    }

    private Multimap<String, ProcessorNode> wireBolt(BoltDeclarer boltDeclarer,
                                                     List<ProcessorNode> curGroup,
                                                     List<ProcessorNode> initialProcessors,
                                                     Map<Node, GroupingInfo> nodeGroupingInfo) {
        Multimap<String, ProcessorNode> streamToInitialProcessor = ArrayListMultimap.create();
        Set<ProcessorNode> curSet = new HashSet<>(curGroup);
        for (ProcessorNode curNode : initialProcessors) {
            for (Node parent : parentNodes(curNode)) {
                if (!curSet.contains(parent)) {
                    GroupingInfo groupingInfo = nodeGroupingInfo.get(parent);
                    if (groupingInfo == null) {
                        boltDeclarer.shuffleGrouping(parent.getComponentId(), parent.getOutputStream());
                    } else if (groupingInfo.getGrouping() == GroupingInfo.Grouping.GLOBAL) {
                        boltDeclarer.globalGrouping(parent.getComponentId(), parent.getOutputStream());
                    } else if (groupingInfo.getGrouping() == GroupingInfo.Grouping.FIELDS) {
                        boltDeclarer.fieldsGrouping(parent.getComponentId(), parent.getOutputStream(),
                                groupingInfo.getFields());
                    }
                    // TODO: put global stream id for spouts
                    streamToInitialProcessor.put(parent.getOutputStream(), curNode);
                }
            }
        }
        return streamToInitialProcessor;
    }

    private List<ProcessorNode> initialProcessors(List<ProcessorNode> curGroup) {
        List<ProcessorNode> nodes = new ArrayList<>();
        // TODO: remove
//        DirectedSubgraph<Node, Edge> subgraph = new DirectedSubgraph<>(graph, new HashSet<Node>(curGroup), null);
//        for (Node pn : subgraph.vertexSet()) {
//            List<ProcessorNode> parents = StreamUtil.getParents(subgraph, pn);
//            if (parents.isEmpty()) {
//                nodes.add((ProcessorNode) pn);
//            }
//        }
        Set<ProcessorNode> curSet = new HashSet<>(curGroup);
        for (ProcessorNode node : curGroup) {
            for (Node parent : parentNodes(node)) {
                if (!curSet.contains(parent)) {
                    nodes.add(node);
                }
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
