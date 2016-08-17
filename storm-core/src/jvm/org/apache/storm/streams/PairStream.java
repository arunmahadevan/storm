package org.apache.storm.streams;

import org.apache.storm.streams.operations.Aggregator;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.PairValueJoiner;
import org.apache.storm.streams.operations.Reducer;
import org.apache.storm.streams.operations.ValueJoiner;
import org.apache.storm.streams.processors.AggregateByKeyProcessor;
import org.apache.storm.streams.processors.FlatMapValuesProcessor;
import org.apache.storm.streams.processors.JoinProcessor;
import org.apache.storm.streams.processors.MapValuesProcessor;
import org.apache.storm.streams.processors.ReduceByKeyProcessor;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class PairStream<K, V> extends Stream<Pair<K, V>> {

    PairStream(StreamBuilder topology, Node node) {
        super(topology, node);
    }

    /**
     * Returns a new stream by applying a {@link Function} to the value of each key-value pairs in
     * this stream.
     *
     * @param function the mapping function
     * @param <R>      the result type
     * @return the new stream
     */
    public <R> PairStream<K, R> mapValues(Function<V, R> function) {
        Node mapValues = streamBuilder.addNode(this, makeProcessorNode(new MapValuesProcessor<>(function), KEY_VALUE));
        return new PairStream<>(streamBuilder, mapValues);
    }

    /**
     * Return a new stream by applying a {@link FlatMapFunction} function to the value of each key-value pairs in
     * this stream.
     *
     * @param function the flatmap function
     * @param <R>      the result type
     * @return the new stream
     */
    public <R> PairStream<K, R> flatMapValues(FlatMapFunction<V, R> function) {
        Node flatMapValues = streamBuilder.addNode(this, makeProcessorNode(new FlatMapValuesProcessor<>(function), KEY_VALUE));
        return new PairStream<>(streamBuilder, flatMapValues);
    }

    /**
     * Aggregates the values for each key of this stream using the given {@link Aggregator}.
     *
     * @param aggregator the aggregator
     * @param <R>        the result type
     * @return the new stream
     */
    public <R> PairStream<K, R> aggregateByKey(Aggregator<V, R> aggregator) {
        Node agg = streamBuilder.addNode(this, makeProcessorNode(new AggregateByKeyProcessor<>(aggregator), KEY_VALUE));
        return new PairStream<>(streamBuilder, agg);
    }

    /**
     * Performs a reduction on the values for each key of this stream by repeatedly applying the reducer.
     *
     * @param reducer the reducer
     * @return the new stream
     */
    public PairStream<K, V> reduceByKey(Reducer<V> reducer) {
        Node reduce = streamBuilder.addNode(this, makeProcessorNode(new ReduceByKeyProcessor<>(reducer), KEY_VALUE));
        return new PairStream<>(streamBuilder, reduce);
    }

    /**
     * Returns a new stream where the values are grouped by the keys.
     *
     * @return the new stream
     */
    public PairStream<K, V> groupByKey() {
        return partitionBy(KEY);
    }

    /**
     * Returns a new stream where the values are grouped by keys and the given window.
     * The values that arrive within a window having the same key will be merged together and returned
     * as an Iterable of values mapped to the key.
     *
     * @param window the window configuration
     * @return the new stream
     */
    public PairStream<K, Iterable<V>> groupByKeyAndWindow(Window<?, ?> window) {
        return groupByKey()
                .window(window)
                .aggregateByKey(new MergeValues<V>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PairStream<K, V> peek(Consumer<Pair<K, V>> action) {
        return toPairStream(super.peek(action));
    }

    /**
     * Join the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism and windowing parameters (if windowed) of this stream is carried forward to the joined stream.
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
     * Join the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism and windowing parameters (if windowed) of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param valueJoiner the {@link ValueJoiner}
     * @param <R>         the type of the values resulting from the join
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <R, V1> PairStream<K, R> join(PairStream<K, V1> otherStream, ValueJoiner<V, V1, R> valueJoiner) {
        String leftStream = stream;
        String rightStream = otherStream.stream;
        Node joinNode = addProcessorNode(new JoinProcessor<>(leftStream, rightStream, valueJoiner), KEY_VALUE);
        streamBuilder.addNode(otherStream, joinNode, joinNode.parallelism);
        return new PairStream<>(streamBuilder, joinNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PairStream<K, V> window(Window<?, ?> window) {
        return toPairStream(super.window(window));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PairStream<K, V> repartition(int parallelism) {
        return toPairStream(super.repartition(parallelism));
    }

    public <R> PairStream<K, R> updateStateByKey(Aggregator<V, R> aggregator) {
        // TODO: execute this via stateful bolt ?
        return null;
    }

    private PairStream<K, V> partitionBy(Fields fields) {
        return new PairStream<>(streamBuilder, addNode(new PartitionNode(
                stream, node.getOutputFields(), GroupingInfo.fields(fields))));
    }

    private PairStream<K, V> toPairStream(Stream<Pair<K, V>> stream) {
        return new PairStream<>(stream.getStreamBuilder(), stream.getNode());
    }

    // used internally to merge values in groupByKeyAndWindow
    private static class MergeValues<V> implements Aggregator<V, Iterable<V>> {
        @Override
        public Iterable<V> init() {
            return new ArrayList<>();
        }

        @Override
        public Iterable<V> apply(V value, Iterable<V> aggregate) {
            List<V> result = new ArrayList<>();
            for (V elmnt : aggregate) {
                result.add(elmnt);
            }
            result.add(value);
            return result;
        }
    }
}
