package org.apache.storm.streams;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.storm.streams.GroupingInfo.Grouping.SHUFFLE;
import static org.apache.storm.streams.GroupingInfo.Grouping.FIELDS;
import static org.apache.storm.streams.GroupingInfo.Grouping.GLOBAL;

public class StreamBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(StreamBuilder.class);
    private final DefaultDirectedGraph<Node, Edge> graph;
    private final Table<Node, String, GroupingInfo> nodeGroupingInfo = HashBasedTable.create();
    private final Map<Node, WindowNode> windowInfo = new HashMap<>();
    private final List<ProcessorNode> curGroup = new ArrayList<>();
    private final List<StreamBolt> streamBolts = new ArrayList<>();
    private String timestampFieldName = null;

    public StreamBuilder() {
        graph = new DefaultDirectedGraph<>(new StreamsEdgeFactory());
    }

    public Stream<Tuple> newStream(IRichSpout spout) {
        return newStream(spout, 1);
    }

    public Stream<Tuple> newStream(IRichSpout spout, int parallelism) {
        SpoutNode spoutNode = new SpoutNode(spout);
        String spoutId = UniqueIdGen.getInstance().getUniqueSpoutId();
        spoutNode.setComponentId(spoutId);
        spoutNode.setParallelism(parallelism);
        graph.addVertex(spoutNode);
        return new Stream<>(this, spoutNode);
    }

    public <T> Stream<T> newStream(IRichSpout spout, TupleValueMapper<T> valueMapper) {
        return newStream(spout).map(valueMapper);
    }

    public <T> Stream<T> newStream(IRichSpout spout, TupleValueMapper<T> valueMapper, int parallelism) {
        return newStream(spout, parallelism).map(valueMapper);
    }

    public StormTopology build() {
        nodeGroupingInfo.clear();
        windowInfo.clear();
        curGroup.clear();
        TopologicalOrderIterator<Node, Edge> iterator = new TopologicalOrderIterator<>(graph);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        while (iterator.hasNext()) {
            Node node = iterator.next();
            if (node instanceof SpoutNode) {
                addSpout(topologyBuilder, (SpoutNode) node);
            } else if (node instanceof ProcessorNode) {
                curGroup.add((ProcessorNode) node);
            } else if (node instanceof PartitionNode) {
                updateNodeGroupingInfo((PartitionNode) node);
                processCurGroup(topologyBuilder);
            } else if (node instanceof WindowNode) {
                updateWindowInfo((WindowNode) node);
                processCurGroup(topologyBuilder);
            } else if (node instanceof SinkNode) {
                processCurGroup(topologyBuilder);
                addSink(topologyBuilder, (SinkNode) node);
            }
        }
        processCurGroup(topologyBuilder);
        mayBeAddTsField();
        return topologyBuilder.createTopology();
    }

    private void mayBeAddTsField() {
        if (timestampFieldName != null) {
            for (StreamBolt streamBolt : streamBolts) {
                streamBolt.setTimestampField(timestampFieldName);
            }
        }
    }

    Node addNode(Stream<?> parent, Node newNode) {
        return addNode(parent, newNode, parent.node.parallelism);
    }

    Node addNode(Stream<?> parent, Node newNode, int parallelism) {
        return addNode(parent, newNode, parallelism, parent.stream);
    }

    Node addNode(Stream<?> parent, Node newNode, int parallelism, String parentStreamId) {
        Node parentNode = parent.getNode();
        graph.addVertex(newNode);
        graph.addEdge(parentNode, newNode);
        newNode.setParallelism(parallelism);
        if (parentNode instanceof WindowNode || parentNode instanceof PartitionNode) {
            newNode.addParentStream(parentNode(parentNode), parentStreamId);
        } else {
            newNode.addParentStream(parentNode, parentStreamId);
        }
        return newNode;
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

    private void updateNodeGroupingInfo(PartitionNode partitionNode) {
        if (partitionNode.getGroupingInfo() != null) {
            for (Node parent : parentNodes(partitionNode)) {
                for (String parentStream : partitionNode.getParentStreams(parent)) {
                    nodeGroupingInfo.put(parent, parentStream, partitionNode.getGroupingInfo());
                }
            }
        }
    }

    private void updateWindowInfo(WindowNode windowNode) {
        for (Node parent : parentNodes(windowNode)) {
            windowInfo.put(parent, windowNode);
        }
        String tsField = windowNode.getWindowParams().getTimestampField();
        if (tsField != null) {
            if (timestampFieldName != null && !tsField.equals(timestampFieldName)) {
                throw new IllegalArgumentException("Cannot set different timestamp field names");
            }
            timestampFieldName = tsField;
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

    private void processCurGroup(TopologyBuilder topologyBuilder) {
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
        StreamBolt bolt;
        if (windowNodes.isEmpty()) {
            bolt = addBolt(topologyBuilder, boltId, initialProcessors);
        } else if (windowNodes.size() == 1) {
            WindowNode windowNode = windowNodes.iterator().next();
            bolt = addWindowedBolt(topologyBuilder, boltId, initialProcessors, windowNode);
        } else {
            throw new IllegalStateException("More than one window config for current group " + curGroup);
        }
        streamBolts.add(bolt);
        curGroup.clear();
    }

    private int getParallelism() {
        Set<Integer> parallelisms = new HashSet<>(Collections2.transform(curGroup, new Function<ProcessorNode, Integer>() {
            @Override
            public Integer apply(ProcessorNode input) {
                return input.getParallelism();
            }
        }));

        if (parallelisms.size() > 1) {
            throw new IllegalStateException("Current group does not have same parallelism " + curGroup);
        }

        return parallelisms.isEmpty() ? 1 : parallelisms.iterator().next();
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

    private void addSpout(TopologyBuilder topologyBuilder, SpoutNode spout) {
        topologyBuilder.setSpout(spout.getComponentId(), spout.getSpout(), spout.getParallelism());
    }

    private void addSink(TopologyBuilder topologyBuilder, SinkNode sinkNode) {
        IComponent bolt = sinkNode.getBolt();
        BoltDeclarer boltDeclarer;
        if (bolt instanceof IRichBolt) {
            boltDeclarer = topologyBuilder.setBolt(sinkNode.getComponentId(), (IRichBolt) bolt, sinkNode.getParallelism());
        } else if (bolt instanceof IBasicBolt) {
            boltDeclarer = topologyBuilder.setBolt(sinkNode.getComponentId(), (IBasicBolt) bolt, sinkNode.getParallelism());
        } else {
            throw new IllegalArgumentException("Expect IRichBolt or IBasicBolt in addBolt");
        }
        for (Node parent : parentNodes(sinkNode)) {
            for (String stream : sinkNode.getParentStreams(parent)) {
                declareStream(boltDeclarer, parent, stream, nodeGroupingInfo.get(parent, stream));
            }
        }
    }

    private StreamBolt addWindowedBolt(TopologyBuilder topologyBuilder,
                                 String boltId,
                                 List<ProcessorNode> initialProcessors,
                                 WindowNode windowNode) {
        WindowedProcessorBolt bolt = new WindowedProcessorBolt(graph, curGroup, windowNode);
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt, getParallelism());
        bolt.setStreamToInitialProcessors(wireBolt(boltDeclarer, initialProcessors));
        return bolt;
    }

    private StreamBolt addBolt(TopologyBuilder topologyBuilder,
                         String boltId,
                         List<ProcessorNode> initialProcessors) {
        ProcessorBolt bolt = new ProcessorBolt(graph, curGroup);
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt, getParallelism());
        bolt.setStreamToInitialProcessors(wireBolt(boltDeclarer, initialProcessors));
        return bolt;
    }

    private Set<String> getWindowedParentStreams(ProcessorNode processorNode) {
        Set<String> res = new HashSet<>();
        for (Node parent : parentNodes(processorNode)) {
            if (parent instanceof ProcessorNode) {
                ProcessorNode pn = (ProcessorNode) parent;
                if (pn.isWindowed()) {
                    res.addAll(Collections2.filter(pn.getOutputStreams(), new Predicate<String>() {
                        @Override
                        public boolean apply(String input) {
                            return !StreamUtil.isSinkStream(input);
                        }
                    }));
                }
            }
        }
        return res;
    }

    private Multimap<String, ProcessorNode> wireBolt(BoltDeclarer boltDeclarer,
                                                     List<ProcessorNode> initialProcessors) {
        LOG.debug("Wiring bolt with boltDeclarer {}, curGroup {}, initialProcessors {}, nodeGroupingInfo {}",
                boltDeclarer, curGroup, initialProcessors, nodeGroupingInfo);
        Multimap<String, ProcessorNode> streamToInitialProcessor = ArrayListMultimap.create();
        Set<ProcessorNode> curSet = new HashSet<>(curGroup);
        for (ProcessorNode curNode : initialProcessors) {
            for (Node parent : parentNodes(curNode)) {
                if (!curSet.contains(parent)) {
                    for (String stream : curNode.getParentStreams(parent)) {
                        declareStream(boltDeclarer, parent, stream, nodeGroupingInfo.get(parent, stream));
                        // TODO: put global stream id for spouts
                        streamToInitialProcessor.put(stream, curNode);
                    }
                } else {
                    LOG.debug("Parent {} of curNode {} is in curGroup {}", parent, curNode, curGroup);
                }
            }
        }
        return streamToInitialProcessor;
    }

    private void declareStream(BoltDeclarer boltDeclarer, Node parent, String streamId, GroupingInfo groupingInfo) {
        if (groupingInfo == null || groupingInfo.getGrouping() == SHUFFLE) {
            boltDeclarer.shuffleGrouping(parent.getComponentId(), streamId);
        } else if (groupingInfo.getGrouping() == FIELDS) {
            boltDeclarer.fieldsGrouping(parent.getComponentId(), streamId,
                    groupingInfo.getFields());
        } else if (groupingInfo.getGrouping() == GLOBAL) {
            boltDeclarer.globalGrouping(parent.getComponentId(), streamId);
        }
    }

    private List<ProcessorNode> initialProcessors(List<ProcessorNode> curGroup) {
        List<ProcessorNode> nodes = new ArrayList<>();
        Set<ProcessorNode> curSet = new HashSet<>(curGroup);
        for (ProcessorNode node : curGroup) {
            for (Node parent : parentNodes(node)) {
                if (!(parent instanceof ProcessorNode) || !curSet.contains(parent)) {
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
            } else {
                return (parent instanceof PartitionNode) && isWindowed(parent);
            }
        }
        return false;
    }
}
