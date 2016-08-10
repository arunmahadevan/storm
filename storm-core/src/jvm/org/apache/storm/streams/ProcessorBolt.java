package org.apache.storm.streams;

import com.google.common.collect.Multimap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.DirectedGraph;

import java.util.List;
import java.util.Map;

class ProcessorBolt extends BaseRichBolt {
    private final ProcessorBoltDelegate delegate;

    public ProcessorBolt(DirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes) {
        delegate = new ProcessorBoltDelegate(graph, nodes);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(Tuple input) {
        RefCountedTuple refCountedTuple = new RefCountedTuple(input);
        delegate.setAnchor(refCountedTuple);
        delegate.process(delegate.getValue(input), input.getSourceStreamId());
        delegate.ack(refCountedTuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }


    public void setStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        delegate.setStreamToInitialProcessors(streamToInitialProcessors);
    }
}
