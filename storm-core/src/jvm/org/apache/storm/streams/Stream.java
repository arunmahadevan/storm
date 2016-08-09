package org.apache.storm.streams;

import org.apache.storm.streams.windowing.Window;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;

// TODO: for event time transparently handle "ts" field
public class Stream<T> {
    protected final StreamBuilder streamBuilder;
    protected final Node node;
    protected final String streamId;

    public Stream(StreamBuilder streamBuilder, Node node) {
        this(streamBuilder, node, node.getOutputStreams().iterator().next());
    }

    public Stream(StreamBuilder streamBuilder, Node node, String streamId) {
        this.streamBuilder = streamBuilder;
        this.node = node;
        this.streamId = streamId;
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
                addProcessorNode(new MapProcessor<>(function), new Fields("value")));
    }

    public <K, V> PairStream<K, V> mapToPair(PairFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder,
                addProcessorNode(new MapProcessor<>(function), new Fields("key", "value")));
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
                addProcessorNode(new FlatMapProcessor<>(function), new Fields("value")));
    }

    public <K, V> PairStream<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder,
                addProcessorNode(new FlatMapProcessor<>(function), new Fields("key", "value")));
    }

    public Stream<T> window(Window<?, ?> window) {
        return new Stream<>(streamBuilder,
                addNode(new WindowNode(window, streamId, node.getOutputFields())));
    }

    // TODO: reduceByWindow?

    public void forEach(Consumer<T> action) {
        addProcessorNode(new ForEachProcessor<>(action), new Fields());
    }

    public Stream<T> peek(Consumer<T> action) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new PeekProcessor<>(action), node.getOutputFields()));
    }

    public <R> Stream<R> aggregate(Aggregator<T, R> aggregator) {
        return new Stream<>(
                streamBuilder,
                global().addProcessorNode(new AggregateProcessor<>(aggregator), new Fields("value")));
    }

    public Stream<T> reduce(Reducer<T> reducer) {
        return new Stream<>(
                streamBuilder,
                global().addProcessorNode(new ReduceProcessor<>(reducer), new Fields("value")));
    }

    /**
     * Returns a new stream with the given value of parallelism. Further operations on this stream
     * would execute at this level of parallelism.
     *
     * @param parallelism the parallelism value
     * @return the new stream
     */
    public Stream<T> repartition(int parallelism) {
        PartitionNode partitionNode = new PartitionNode(streamId, node.getOutputFields(), null);
        return new Stream<>(streamBuilder, addNode(partitionNode, parallelism));
    }

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
        addNode(sinkNode, parallelism, node.addOutputStream(StreamUtil.getSinkStream(streamId)));
    }

    private Node addNode(Node node, int parallelism) {
        return streamBuilder.addNode(this, node, parallelism);
    }

    private Node addNode(Node node, int parallelism, String parentStreamId) {
        return streamBuilder.addNode(this, node, parallelism, parentStreamId);
    }

    private Stream<T> global() {
        return new Stream<>(streamBuilder,
                addNode(new PartitionNode(streamId, node.getOutputFields(), GroupingInfo.global())));
    }
}
