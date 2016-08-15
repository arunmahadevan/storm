package org.apache.storm.streams;

import org.apache.storm.streams.operations.Aggregator;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.PairFlatMapFunction;
import org.apache.storm.streams.operations.PairFunction;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.operations.PrintConsumer;
import org.apache.storm.streams.operations.Reducer;
import org.apache.storm.streams.processors.AggregateProcessor;
import org.apache.storm.streams.processors.FilterProcessor;
import org.apache.storm.streams.processors.FlatMapProcessor;
import org.apache.storm.streams.processors.ForEachProcessor;
import org.apache.storm.streams.processors.MapProcessor;
import org.apache.storm.streams.processors.PeekProcessor;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.streams.processors.ReduceProcessor;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;

// TODO: for event time transparently handle "ts" field
public class Stream<T> {
    public static final String FIELD_KEY = "key";
    public static final String FIELD_VALUE = "value";

    // the stream builder
    protected final StreamBuilder streamBuilder;
    // the current node
    protected final Node node;
    // the stream id from node's output stream(s) that this stream represents
    protected final String stream;

    Stream(StreamBuilder streamBuilder, Node node) {
        this(streamBuilder, node, node.getOutputStreams().iterator().next());
    }

    private Stream(StreamBuilder streamBuilder, Node node, String stream) {
        this.streamBuilder = streamBuilder;
        this.node = node;
        this.stream = stream;
    }

    /**
     * Returns a stream consisting of the elements of this stream that matches the given filter.
     *
     * @param predicate the predicate to apply to each element to determine if it should be included
     * @return the new stream
     */
    public Stream<T> filter(Predicate<T> predicate) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new FilterProcessor<>(predicate), node.getOutputFields()));
    }

    /**
     * Returns a stream consisting of the result of applying the given mapping function to the values of this stream.
     *
     * @param function a mapping function to be applied to each value in this stream.
     * @return the new stream
     */
    public <R> Stream<R> map(Function<T, R> function) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new MapProcessor<>(function), new Fields(FIELD_VALUE)));
    }

    /**
     * Returns a stream of key-value pairs by applying a {@link PairFunction} on each value of this stream.
     *
     * @param function the mapping function to be applied to each value in this stream
     * @param <K>      the key type
     * @param <V>      the value type
     * @return the new stream of key-value pairs
     */
    public <K, V> PairStream<K, V> mapToPair(PairFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder,
                addProcessorNode(new MapProcessor<>(function), new Fields(FIELD_KEY, FIELD_VALUE)));
    }

    /**
     * Returns a stream consisting of the results of replacing each value of this stream with the contents
     * produced by applying the provided mapping function to each value. This has the effect of applying
     * a one-to-many transformation to the values of the stream, and then flattening the resulting elements into a new stream.
     *
     * @param function a mapping function to be applied to each value in this stream which produces new values.
     * @return the new stream
     */
    public <R> Stream<R> flatMap(FlatMapFunction<T, R> function) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new FlatMapProcessor<>(function), new Fields(FIELD_VALUE)));
    }

    /**
     * Returns a stream consisting of the results of replacing each value of this stream with the key-value pairs
     * produced by applying the provided mapping function to each value.
     *
     * @param function the mapping function to be applied to each value in this stream which produces new key-value pairs.
     * @param <K>      the key type
     * @param <V>      the value type
     * @return the new stream of key-value pairs
     * @see #flatMap(FlatMapFunction)
     * @see #mapToPair(PairFunction)
     */
    public <K, V> PairStream<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder,
                addProcessorNode(new FlatMapProcessor<>(function), new Fields(FIELD_KEY, FIELD_VALUE)));
    }

    /**
     * Returns a new stream consisting of the elements that fall within the window as specified by the window parameter. The
     * {@link Window} specification could be used to specify sliding or tumbling windows based on time duration or event count.
     * For example,
     * <pre>
     * // time duration based sliding window
     * stream.window(SlidingWindows.of(Duration.minutes(10), Duration.minutes(1));
     *
     * // count based sliding window
     * stream.window(SlidingWindows.of(Count.(10), Count.of(2)));
     *
     * // time duration based tumbling window
     * stream.window(TumblingWindows.of(Duration.seconds(10));
     * </p>
     *
     * @see org.apache.storm.streams.windowing.SlidingWindows
     * @see org.apache.storm.streams.windowing.TumblingWindows
     * @param window the window configuration
     * @return the new stream
     */
    public Stream<T> window(Window<?, ?> window) {
        return new Stream<>(streamBuilder,
                addNode(new WindowNode(window, stream, node.getOutputFields())));
    }

    /**
     * Performs an action for each element of this stream.
     *
     * @param action an action to perform on the elements
     */
    public void forEach(Consumer<T> action) {
        addProcessorNode(new ForEachProcessor<>(action), new Fields());
    }

    /**
     * Returns a stream consisting of the elements of this stream, additionally performing the provided action on
     * each element as they are consumed from the resulting stream.
     *
     * @param action the action to perform on the element as they are consumed from the stream
     * @return the new stream
     */
    public Stream<T> peek(Consumer<T> action) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new PeekProcessor<>(action), node.getOutputFields()));
    }

    /**
     * Aggregates the values in this stream using the aggregator. This does a global aggregation, i.e. the elements
     * across all the partitions are forwarded to a single task for computing the aggregate.
     * <p>
     * If the stream is windowed, the aggregate result is emitted after each window activation and represents the
     * aggregate of elements that fall within that window.
     * If the stream is not windowed, the aggregate result is emitted as each new element in the stream is processed.
     * </p>
     *
     * @param aggregator the aggregator
     * @param <R>        the result type
     * @return the new stream
     */
    public <R> Stream<R> aggregate(Aggregator<T, R> aggregator) {
        return new Stream<>(
                streamBuilder,
                global().addProcessorNode(new AggregateProcessor<>(aggregator), new Fields(FIELD_VALUE)));
    }

    /**
     * Performs a reduction on the elements of this stream, by repeatedly applying the reducer.
     * <p>
     * If the stream is windowed, the result is emitted after each window activation and represents the
     * reduction of elements that fall within that window.
     * If the stream is not windowed, the result is emitted as each new element in the stream is processed.
     * </p>
     *
     * @param reducer the reducer
     * @return the new stream
     */
    public Stream<T> reduce(Reducer<T> reducer) {
        return new Stream<>(
                streamBuilder,
                global().addProcessorNode(new ReduceProcessor<>(reducer), new Fields(FIELD_VALUE)));
    }

    /**
     * Returns a new stream with the given value of parallelism. Further operations on this stream
     * would execute at this level of parallelism.
     *
     * @param parallelism the parallelism value
     * @return the new stream
     */
    public Stream<T> repartition(int parallelism) {
        return new Stream<>(streamBuilder,
                addNode(new PartitionNode(stream, node.getOutputFields(), null), parallelism));
    }

    /**
     * Print the values in this stream.
     */
    public void print() {
        forEach(new PrintConsumer<T>());
    }

    public void to(IRichBolt bolt) {
        to(bolt, 1);
    }

    public void to(IRichBolt bolt, int parallelism) {
        addSinkNode(new SinkNode(bolt), parallelism);
    }

    public void to(IBasicBolt bolt) {
        to(bolt, 1);
    }

    public void to(IBasicBolt bolt, int parallelism) {
        addSinkNode(new SinkNode(bolt), parallelism);
    }

    Node getNode() {
        return node;
    }

    StreamBuilder getStreamBuilder() {
        return streamBuilder;
    }

    Node addNode(Node node) {
        return streamBuilder.addNode(this, node);
    }

    Node addProcessorNode(Processor<?> processor, Fields outputFields) {
        return addNode(makeProcessorNode(processor, outputFields));
    }

    ProcessorNode makeProcessorNode(Processor<?> processor, Fields outputFields) {
        return new ProcessorNode(processor, UniqueIdGen.getInstance().getUniqueStreamId()
                , outputFields);
    }

    private void addSinkNode(SinkNode sinkNode, int parallelism) {
        String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
        sinkNode.setComponentId(boltId);
        sinkNode.setParallelism(parallelism);
        addNode(sinkNode, parallelism, node.addOutputStream(StreamUtil.getSinkStream(stream)));
    }

    private Node addNode(Node node, int parallelism) {
        return streamBuilder.addNode(this, node, parallelism);
    }

    private Node addNode(Node node, int parallelism, String parentStreamId) {
        return streamBuilder.addNode(this, node, parallelism, parentStreamId);
    }

    private Stream<T> global() {
        return new Stream<>(streamBuilder,
                addNode(new PartitionNode(stream, node.getOutputFields(), GroupingInfo.global())));
    }
}
