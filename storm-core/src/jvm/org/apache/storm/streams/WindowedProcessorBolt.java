package org.apache.storm.streams;

import com.google.common.collect.Multimap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

public class WindowedProcessorBolt extends BaseWindowedBolt {
    private final ProcessorBoltDelegate delegate;
    private final WindowNode windowNode;


    public WindowedProcessorBolt(DefaultDirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes, WindowNode windowNode) {
        delegate = new ProcessorBoltDelegate(graph, nodes);
        this.windowNode = windowNode;
        setWindowParams();
    }

    private void setWindowParams() {
        // TODO:
        withTumblingWindow(Duration.seconds(2));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        // TODO: check anchoring/acking
        for (Tuple tuple : inputWindow.get()) {
            delegate.setAnchor(tuple);
            Object value = delegate.getValue(tuple);
            if (!delegate.isPunctuation(value)) {
                delegate.process(value, tuple.getSourceStreamId());
            }
        }

        for (String stream : delegate.getInitialStreams()) {
            delegate.process(PUNCTUATION, stream);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    public void setStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        delegate.setStreamToInitialProcessors(streamToInitialProcessors);
    }
}
