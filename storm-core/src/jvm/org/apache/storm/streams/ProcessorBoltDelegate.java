package org.apache.storm.streams;

import com.google.common.collect.Multimap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ProcessorBoltDelegate implements Serializable {
    private final DirectedGraph<Node, Edge> graph;
    private final Set<ProcessorNode> nodes;
    private Map stormConf;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
    private List<ProcessorNode> outgoingProcessors = new ArrayList<>();
    private Set<EmittingProcessorContext> emittingProcessorContexts = new HashSet<>();
    Map<ProcessorNode, Set<String>> punctuationState = new HashMap<>();

    private Multimap<String, ProcessorNode> streamToInitialProcessors;

    ProcessorBoltDelegate(DirectedGraph<Node, Edge> graph, Set<ProcessorNode> nodes) {
        this.graph = graph;
        this.nodes = new HashSet<>(nodes);
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
                EmittingProcessorContext emittingProcessorContext =
                        new EmittingProcessorContext(processorNode, collector);
                outgoingProcessors.add(processorNode);
                emittingProcessorContexts.add(emittingProcessorContext);
                processorContext = emittingProcessorContext;
            } else {
                processorContext = new ForwardingProcessorContext(processorNode, children);
            }
            processorNode.initProcessorContext(processorContext);
        }
    }

    void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (ProcessorNode node : nodes) {
            declarer.declareStream(node.getOutputStream(), node.getOutputFields());
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
        outputCollector.ack(tuple);
    }

    void process(Object value, String sourceStreamId) {
        Collection<ProcessorNode> initialProcessors = streamToInitialProcessors.get(sourceStreamId);
        Iterator<ProcessorNode> it = initialProcessors.iterator();
        while (it.hasNext()) {
            ProcessorNode processorNode = it.next();
            Processor processor = processorNode.getProcessor();
            if (isPunctuation(value)) {
                if (shouldPunctuate(processorNode, sourceStreamId)) {
                    processor.punctuate(null);
                    clearPunctuationState(processorNode);
                }
            } else {
                processor.execute(value);
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
