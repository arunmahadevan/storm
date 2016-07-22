package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.jgrapht.DirectedGraph;

import java.util.Map;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

public class WindowedProcessorBolt extends BaseWindowedBolt {
    private final DirectedGraph<Node, Edge> graph;
    private final WindowNode node;
    private OutputCollector collector;

    public WindowedProcessorBolt(DirectedGraph<Node, Edge> graph, WindowNode node) {
        this.graph = graph;
        this.node = node;
        setWindowParams();
    }

    private void setWindowParams() {
        // TODO:
        withTumblingWindow(Duration.seconds(2));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        for (Tuple tuple : inputWindow.get()) {
            collector.emit(node.getOutputStream(), tuple.getValues());
        }
        collector.emit(node.getOutputStream(), punctuation());
    }

    private Values punctuation() {
        Values values = new Values();
        for (int i = 0; i < node.getOutputFields().size(); i++) {
            values.add(PUNCTUATION);
        }
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(node.getOutputStream(), node.getOutputFields());
    }
}
