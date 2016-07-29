package org.apache.storm.streams;

import org.apache.storm.streams.windowing.Window;
import org.apache.storm.tuple.Fields;

public class PairStream<K, V> extends Stream<Pair<K, V>> {

    public PairStream(StreamBuilder topology, Node node) {
        super(topology, node);
    }

    public <R> PairStream<K, R> mapValues(Function<V, R> function) {
        Node mapValues = streamBuilder.addNode(this, makeProcessorNode(new MapValuesProcessor<>(function), new Fields("key", "value")));
        return new PairStream<>(streamBuilder, mapValues);
    }

    public <R> PairStream<K, R> aggregateByKey(Aggregator<V, R> aggregator) {
        Node agg = streamBuilder.addNode(this, makeProcessorNode(new AggregateByKeyProcessor<>(aggregator), new Fields("key", "value")));
        return new PairStream<>(streamBuilder, agg);
    }

    public PairStream<K, V> reduceByKey(Reducer<V> reducer) {
        Node reduce = streamBuilder.addNode(this, makeProcessorNode(new ReduceByKeyProcessor<>(reducer), new Fields("key", "value")));
        return new PairStream<>(streamBuilder, reduce);
    }

    public PairStream<K, V> groupByKey() {
        return partitionBy(new Fields("key"));
    }

    @Override
    public PairStream<K, V> peek(Consumer<Pair<K, V>> action) {
        return new PairStream<>(
                streamBuilder,
                addProcessorNode(new PeekProcessor<>(action), new Fields("key", "value")));
    }

    public <R, V1> PairStream<K, R> join(PairStream<K, V1> otherStream, ValueJoiner<V, V1, R> valueJoiner) {
        String leftStream = getJoinStream(node);
        String rightStream = getJoinStream(otherStream.node);

        Node joinNode = addProcessorNode(new JoinProcessor<>(leftStream, rightStream, valueJoiner), new Fields("key", "value"));
        streamBuilder.addNode(otherStream, joinNode);
        return new PairStream<>(streamBuilder, joinNode);
    }

    private String getJoinStream(Node node) {
        if (node instanceof WindowNode || node instanceof PartitionNode) {
            node = streamBuilder.parentNode(node);
        }
        return node.getOutputStream();
    }

    public <V1> PairStream<K, Pair<V, V1>> join(PairStream<K, V1> otherStream) {
        return join(otherStream, new PairValueJoiner<V, V1>());
    }


    @Override
    public PairStream<K, V> window(Window<?, ?> window) {
        return new PairStream<>(streamBuilder,
                addNode(new WindowNode(window, UniqueIdGen.getInstance().getUniqueStreamId(), node.getOutputFields())));
    }

    PairStream<K, V> partitionBy(Fields fields) {
        return new PairStream<>(streamBuilder, addNode(new PartitionNode(
                node.getOutputStream(), node.getOutputFields(), GroupingInfo.fields(fields))));
    }
}
