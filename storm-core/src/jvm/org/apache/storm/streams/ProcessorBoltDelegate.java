package org.apache.storm.streams;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ProcessorBoltDelegate implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessorBoltDelegate.class);

    private final DirectedGraph<Node, Edge> graph;
    private final List<ProcessorNode> nodes;
    private Map stormConf;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
    private List<ProcessorNode> outgoingProcessors = new ArrayList<>();
    private Set<EmittingProcessorContext> emittingProcessorContexts = new HashSet<>();
    Map<ProcessorNode, Set<String>> punctuationState = new HashMap<>();

    private Multimap<String, ProcessorNode> streamToInitialProcessors;

    ProcessorBoltDelegate(DirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes) {
        this.graph = graph;
        this.nodes = new ArrayList<>(nodes);
    }

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.stormConf = stormConf;
        this.topologyContext = context;
        this.outputCollector = collector;
        DirectedSubgraph<Node, Edge> subgraph = new DirectedSubgraph<>(graph, new HashSet<Node>(nodes), null);
        TopologicalOrderIterator<Node, Edge> it = new TopologicalOrderIterator<>(subgraph);
        while (it.hasNext()) {
            Node node = it.next();
            if (!(node instanceof ProcessorNode)) {
                throw new IllegalStateException("Not a processor node " + node);
            }
            ProcessorNode processorNode = (ProcessorNode) node;
            List<ProcessorNode> children = StreamUtil.getChildren(subgraph, processorNode);
            ProcessorContext processorContext;
            if (children.isEmpty()) {
                processorContext = createEmittingContext(processorNode);
            } else {
                Multimap<String, ProcessorNode> streamToChildren = ArrayListMultimap.create();
                for (ProcessorNode child : children) {
                    for (String stream : child.getParentStreams(processorNode)) {
                        streamToChildren.put(stream, child);
                    }
                }
                ForwardingProcessorContext forwardingContext = new ForwardingProcessorContext(processorNode, streamToChildren);
                if (hasOutgoingChild(processorNode, new HashSet<>(children))) {
                    processorContext = new ChainedProcessorContext(processorNode, forwardingContext, createEmittingContext(processorNode));
                } else {
                    processorContext = forwardingContext;
                }
            }
            processorNode.initProcessorContext(processorContext);
        }
    }

    private ProcessorContext createEmittingContext(ProcessorNode processorNode) {
        List<EmittingProcessorContext> emittingContexts = new ArrayList<>();
        for (String stream : processorNode.getOutputStreams()) {
            EmittingProcessorContext emittingContext = new EmittingProcessorContext(processorNode, outputCollector, stream);
            if (StreamUtil.isSinkStream(stream)) {
                emittingContext.setEmitPunctuation(false);
            }
            emittingContexts.add(emittingContext);
        }
        emittingProcessorContexts.addAll(emittingContexts);
        outgoingProcessors.add(processorNode);
        return new ChainedProcessorContext(processorNode, emittingContexts);
    }

    private boolean hasOutgoingChild(ProcessorNode processorNode, Set<ProcessorNode> boltChildren) {
        for (Node child : getChildNodes(processorNode)) {
            if (child instanceof  ProcessorNode && !boltChildren.contains(child)) {
                return true;
            } else if (child instanceof SinkNode) {
                return true;
            }
        }
        return false;
    }

    private Set<Node> getChildNodes(Node node) {
        Set<Node> children = new HashSet<>();
        for (Node child : StreamUtil.<Node>getChildren(graph, node)) {
            if (child instanceof WindowNode || child instanceof PartitionNode) {
                children.addAll(getChildNodes(child));
            } else {
                children.add(child);
            }
        }
        return children;
    }

    void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (ProcessorNode node : nodes) {
            for (String stream : node.getOutputStreams()) {
                declarer.declareStream(stream, node.getOutputFields());
            }
        }
    }

    void setAnchor(Tuple input) {
        for (EmittingProcessorContext ctx : emittingProcessorContexts) {
            ctx.setAnchor(input);
        }
    }

    Object getValue(Tuple input) {
        Object value;
        //TODO: find a better way
        // if tuple arrives from a spout, it can be passed as is
        // otherwise the value is in the first field of the tuple
        if (input.getSourceComponent().startsWith("spout")) {
            value = input;
        } else if (input.size() == 2) {
            value = new Pair<>(input.getValue(0), input.getValue(1));
        } else {
            value = input.getValue(0);
        }
        return value;
    }

    void ack(Tuple tuple) {
        LOG.debug("ACK tuple {}", tuple);
        outputCollector.ack(tuple);
    }

    void process(Object value, String sourceStreamId) {
        LOG.debug("Process value {}, sourceStreamId {}", value, sourceStreamId);
        Collection<ProcessorNode> initialProcessors = streamToInitialProcessors.get(sourceStreamId);
        for (ProcessorNode processorNode : initialProcessors) {
            Processor processor = processorNode.getProcessor();
            if (isPunctuation(value)) {
                if (shouldPunctuate(processorNode, sourceStreamId)) {
                    processor.punctuate(null);
                    clearPunctuationState(processorNode);
                }
            } else {
                processor.execute(value, sourceStreamId);
            }
        }
    }

    void setStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        this.streamToInitialProcessors = streamToInitialProcessors;
    }

    Set<String> getInitialStreams() {
        return streamToInitialProcessors.keySet();
    }

    // if we received punctuation from all parent windowed streams
    private boolean shouldPunctuate(ProcessorNode processorNode, String sourceStreamId) {
        if (processorNode.getWindowedParentStreams().size() <= 1) {
            return true;
        }
        Set<String> receivedStreams;
        if ((receivedStreams = punctuationState.get(processorNode)) == null) {
            receivedStreams = new HashSet<>();
            punctuationState.put(processorNode, receivedStreams);
        }
        receivedStreams.add(sourceStreamId);
        return receivedStreams.equals(processorNode.getWindowedParentStreams());
    }

    private void clearPunctuationState(ProcessorNode processorNode) {
        Set<String> state;
        if ((state = punctuationState.get(processorNode)) != null) {
            state.clear();
        }
    }

    boolean isPunctuation(Object value) {
        if (value instanceof Pair) {
            value = ((Pair) value).getFirst();
        }
        return WindowNode.PUNCTUATION.equals(value);
    }
}
