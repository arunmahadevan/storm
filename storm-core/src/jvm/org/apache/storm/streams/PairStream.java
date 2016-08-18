package org.apache.storm.streams;

import org.apache.storm.streams.operations.Aggregator;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.PairValueJoiner;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.operations.Reducer;
import org.apache.storm.streams.operations.ValueJoiner;
import org.apache.storm.streams.processors.AggregateByKeyProcessor;
import org.apache.storm.streams.processors.FlatMapValuesProcessor;
import org.apache.storm.streams.processors.JoinProcessor;
import org.apache.storm.streams.processors.MapValuesProcessor;
import org.apache.storm.streams.processors.ReduceByKeyProcessor;
import org.apache.storm.streams.processors.UpdateStateByKeyProcessor;
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
    public <R> PairStream<K, R> mapValues(Function<? super V, ? extends R> function) {
        return new PairStream<>(streamBuilder, addProcessorNode(new MapValuesProcessor<>(function), KEY_VALUE));
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
        return new PairStream<>(streamBuilder, addProcessorNode(new FlatMapValuesProcessor<>(function), KEY_VALUE));
    }

    /**
     * Aggregates the values for each key of this stream using the given {@link Aggregator}.
     *
     * @param aggregator the aggregator
     * @param <R>        the result type
     * @return the new stream
     */
    public <R> PairStream<K, R> aggregateByKey(Aggregator<? super V, ? extends R> aggregator) {
        return new PairStream<>(streamBuilder, addProcessorNode(new AggregateByKeyProcessor<>(aggregator), KEY_VALUE));
    }

    /**
     * Performs a reduction on the values for each key of this stream by repeatedly applying the reducer.
     *
     * @param reducer the reducer
     * @return the new stream
     */
    public PairStream<K, V> reduceByKey(Reducer<V> reducer) {
        return new PairStream<>(streamBuilder, addProcessorNode(new ReduceByKeyProcessor<>(reducer), KEY_VALUE));
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
        return groupByKey().window(window).aggregateByKey(new MergeValues<V>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PairStream<K, V> peek(Consumer<? super Pair<K, V>> action) {
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
    public <R, V1> PairStream<K, R> join(PairStream<K, V1> otherStream,
                                         ValueJoiner<? super V, ? super V1, ? extends R> valueJoiner) {
        String leftStream = stream;
        String rightStream = otherStream.stream;
        Node joinNode = addProcessorNode(new JoinProcessor<>(leftStream, rightStream, valueJoiner), KEY_VALUE);
        addNode(otherStream.getNode(), joinNode, joinNode.getParallelism());
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

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PairStream<K, V>> branch(Predicate<Pair<K, V>>... predicates) {
        List<PairStream<K, V>> pairStreams = new ArrayList<>();
        for (Stream<Pair<K, V>> stream : super.branch(predicates)) {
            pairStreams.add(toPairStream(stream));
        }
        return pairStreams;
    }

    /*
    TODO:
     optionally take a state name so it goes to a global namespace, otherwise put it into task namespace
     execute via stateful bolt
     return the updated key-value pairs?
     e.g       nathan -> 2
               golda  -> 1
               jackson -> 5

               -> (nathan, 1), (golda, 1) -->    nathan  -> 3  --> (nathan, 3), (golda, 2)
                                                 golda   -> 2
                                                 jackson -> 5
     state query can be added later
     */
    public <R> PairStream<K, R> updateStateByKey(Aggregator<? super V, ? extends R> aggregator) {
        return new PairStream<>(streamBuilder, addProcessorNode(new UpdateStateByKeyProcessor<>(aggregator), KEY_VALUE));
    }

    private PairStream<K, V> partitionBy(Fields fields) {
        return new PairStream<>(
                streamBuilder,
                addNode(new PartitionNode(stream, node.getOutputFields(), GroupingInfo.fields(fields))));
    }

    private PairStream<K, V> toPairStream(Stream<Pair<K, V>> stream) {
        return new PairStream<>(stream.streamBuilder, stream.node);
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
