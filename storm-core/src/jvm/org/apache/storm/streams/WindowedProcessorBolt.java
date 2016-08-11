package org.apache.storm.streams;

import com.google.common.collect.Multimap;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

public class WindowedProcessorBolt extends BaseWindowedBolt implements StreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedProcessorBolt.class);

    private final ProcessorBoltDelegate delegate;
    private final WindowNode windowNode;


    public WindowedProcessorBolt(DefaultDirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes, WindowNode windowNode) {
        delegate = new ProcessorBoltDelegate(graph, nodes);
        this.windowNode = windowNode;
        setWindowParams();
    }

    private void setWindowParams() {
        Window<?, ?> window = windowNode.getWindowParams();
        if (window instanceof SlidingWindows) {
            setSlidingWindowParams(window.getWindowLength(), window.getSlidingInterval());
        } else if (window instanceof TumblingWindows) {
            setTumblingWindowParams(window.getWindowLength());
        }
        if (window.getTimestampField() != null) {
            withTimestampField(window.getTimestampField());
        }
        if (window.getLag() != null) {
            withLag(window.getLag());
        }
        if (window.getLateTupleStream() != null) {
            withLateTupleStream(window.getLateTupleStream());
        }
    }

    private void setSlidingWindowParams(Object windowLength, Object slidingInterval) {
        if (windowLength instanceof Count) {
            if (slidingInterval instanceof Count) {
                withWindow((Count) windowLength, (Count) slidingInterval);
            } else if (slidingInterval instanceof Duration) {
                withWindow((Count) windowLength, (Duration) slidingInterval);
            }
        } else if (windowLength instanceof Duration) {
            if (slidingInterval instanceof Count) {
                withWindow((Duration) windowLength, (Count) slidingInterval);
            } else if (slidingInterval instanceof Duration) {
                withWindow((Duration) windowLength, (Duration) slidingInterval);
            }
        }
    }

    private void setTumblingWindowParams(Object windowLength) {
        if (windowLength instanceof Count) {
            withTumblingWindow((Count) windowLength);
        } else if (windowLength instanceof Duration) {
            withTumblingWindow((Duration) windowLength);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        LOG.trace("Window triggered at {}, inputWindow {}", new Date(), inputWindow);
        if (delegate.isEventTimestamp()) {
            delegate.setEventTimestamp(inputWindow.getTimestamp());
        }
        for (Tuple tuple : inputWindow.get()) {
            Object value = delegate.getValue(tuple);
            if (!StreamUtil.isPunctuation(value)) {
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

    @Override
    public void setTimestampField(String fieldName) {
        delegate.setTimestampField(fieldName);
    }
}
