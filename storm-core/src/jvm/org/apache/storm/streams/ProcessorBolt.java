package org.apache.storm.streams;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

class ProcessorBolt extends BaseRichBolt {
    private final List<ProcessorNode> processorNodes;
    private OutputCollector outputCollector;
    private DefaultProcessorContext ctx = new DefaultProcessorContext();

    public ProcessorBolt(List<ProcessorNode> processorNodes) {
        this.processorNodes = processorNodes;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Object result;
        //TODO: find a better way
        // if tuple arrives from a spout, it can be passed as is
        // otherwise the value is in the first field of the tuple
        if (input.getSourceComponent().startsWith("spout")) {
            result = input;
        } else {
            result = input.getValue(0);
        }
        Iterator<ProcessorNode> it = processorNodes.iterator();
        while (it.hasNext() && result != null) {
            Processor processor = it.next().getProcessor();
            processor.execute(result, ctx);
            result = ctx.get();
        }
        if (result != null) {
            outputCollector.emit(input, new Values(result));
            outputCollector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(processorNodes.get(processorNodes.size() - 1).getOutputFields());
    }
}
