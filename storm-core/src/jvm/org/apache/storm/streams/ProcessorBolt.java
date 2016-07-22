package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ProcessorBolt extends BaseRichBolt {
    private final DirectedGraph<Node, Edge> graph;
    private final Set<ProcessorNode> nodes;
    private Map stormConf;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
    private List<ProcessorNode> initialProcessors = new ArrayList<>();
    private List<ProcessorNode> outgoingProcessors = new ArrayList<>();
    private Set<EmittingProcessorContext> emittingProcessorContexts = new HashSet<>();
    // TODO
    private boolean emitGroupKey = false;

    public ProcessorBolt(DirectedGraph<Node, Edge> graph, Set<ProcessorNode> nodes) {
        this.graph = graph;
        this.nodes = nodes;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
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
            List<ProcessorNode> parents = StreamUtil.getParents(subgraph, processorNode);
            List<ProcessorNode> children = StreamUtil.getChildren(subgraph, processorNode);
            if (parents.isEmpty()) {
                initialProcessors.add(processorNode);
            }
            ProcessorContext processorContext;
            if (children.isEmpty()) {
                EmittingProcessorContext emittingProcessorContext =
                        new EmittingProcessorContext(
                                processorNode.getOutputStream(),
                                collector,
                                processorNode.getOutputFields(),
                                processorNode.isWindowed());
                outgoingProcessors.add(processorNode);
                emittingProcessorContexts.add(emittingProcessorContext);
                processorContext = emittingProcessorContext;
            } else {
                processorContext = new ForwardingProcessorContext(children, processorNode.isWindowed());
            }
            processorNode.initProcessorContext(processorContext);
        }
    }

    @Override
    public void execute(Tuple input) {
        setAnchor(input);
        process(input);
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (ProcessorNode node : nodes) {
            declarer.declareStream(node.getOutputStream(), node.getOutputFields());
        }
    }

    private void setAnchor(Tuple input) {
        for (EmittingProcessorContext ctx : emittingProcessorContexts) {
            ctx.setAnchor(input);
        }
    }

    private void process(Tuple input) {
        Object value;
        //TODO: find a better way
        // if tuple arrives from a spout, it can be passed as is
        // otherwise the value is in the first field of the tuple
        if (input.getSourceComponent().startsWith("spout")) {
            value = input;
        } else {
            value = input.getValue(0);
        }

        Iterator<ProcessorNode> it = initialProcessors.iterator();
        while (it.hasNext()) {
            ProcessorNode processorNode = it.next();
            Processor processor = processorNode.getProcessor();
            if (shouldPunctuate(processorNode, value)) {
                processor.punctuate();
            } else {
                // TODO:
                if (input.size() == 2) {
                    value = new Pair<>(input.getValue(0), input.getValue(1));
                }
                processor.execute(value);
            }
        }
    }

    // TODO: punctuation has arrived from all parent processors
    private boolean shouldPunctuate(ProcessorNode processorNode, Object value) {
        return WindowNode.PUNCTUATION.equals(value);
    }

}
