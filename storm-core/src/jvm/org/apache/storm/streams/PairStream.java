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
        return toPairStream(super.peek(action));
    }

    /**
     * Join this values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <V1> PairStream<K, Pair<V, V1>> join(PairStream<K, V1> otherStream) {
        return join(otherStream, new PairValueJoiner<V, V1>());
    }

    /**
     * Join this values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param valueJoiner the {@link ValueJoiner}
     * @param <R>         the type of the values resulting from the join
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <R, V1> PairStream<K, R> join(PairStream<K, V1> otherStream, ValueJoiner<V, V1, R> valueJoiner) {
        String leftStream = getJoinStream(node);
        String rightStream = getJoinStream(otherStream.node);
        Node joinNode = addProcessorNode(new JoinProcessor<>(leftStream, rightStream, valueJoiner), new Fields("key", "value"));
        streamBuilder.addNode(otherStream, joinNode, joinNode.parallelism);
        return new PairStream<>(streamBuilder, joinNode);
    }

    @Override
    public PairStream<K, V> window(Window<?, ?> window) {
        return toPairStream(super.window(window));
    }

    @Override
    public PairStream<K, V> repartition(int parallelism) {
        return toPairStream(super.repartition(parallelism));
    }

    private String getJoinStream(Node node) {
        if (node instanceof WindowNode || node instanceof PartitionNode) {
            node = streamBuilder.parentNode(node);
        }
        return node.getOutputStream();
    }

    private PairStream<K, V> partitionBy(Fields fields) {
        return new PairStream<>(streamBuilder, addNode(new PartitionNode(
                node.getOutputStream(), node.getOutputFields(), GroupingInfo.fields(fields))));
    }

    private PairStream<K, V> toPairStream(Stream<Pair<K, V>> stream) {
        return new PairStream<>(stream.getStreamBuilder(), stream.getNode());
    }
}
