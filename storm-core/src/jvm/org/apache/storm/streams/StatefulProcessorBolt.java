package org.apache.storm.streams;

import com.google.common.collect.Multimap;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.processors.StatefulProcessor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.DirectedGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StatefulProcessorBolt<K, V> extends BaseStatefulBolt<KeyValueState<K, V>> implements StreamBolt {
    private final ProcessorBoltDelegate delegate;
    private final StatefulProcessor statefulProcessor;

    public StatefulProcessorBolt(DirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes) {
        delegate = new ProcessorBoltDelegate(graph, nodes);
        statefulProcessor = getStatefulProcessor(nodes);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(Tuple input) {
        RefCountedTuple refCountedTuple = new RefCountedTuple(input);
        delegate.setAnchor(refCountedTuple);
        Object value = delegate.getValue(input);
        delegate.process(value, input.getSourceStreamId());
        delegate.ack(refCountedTuple);
    }

    @Override
    public void initState(KeyValueState<K, V> state) {
        statefulProcessor.initState(state);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public void setTimestampField(String fieldName) {
        delegate.setTimestampField(fieldName);
    }

    public void setStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        delegate.setStreamToInitialProcessors(streamToInitialProcessors);
    }

    private StatefulProcessor getStatefulProcessor(List<ProcessorNode> nodes) {
        List<StatefulProcessor> statefulProcessors = new ArrayList<>();
        for (ProcessorNode node : nodes) {
            if (node.getProcessor() instanceof StatefulProcessor) {
                statefulProcessors.add((StatefulProcessor) node.getProcessor());
            }
        }
        if (statefulProcessors.size() > 1) {
            throw new IllegalArgumentException("Cannot have more than one stateful processor in a StatefulProcessorBolt");
        }
        return statefulProcessors.get(0);
    }

}
