package org.apache.storm.streams;

import org.apache.storm.streams.windowing.Window;
import org.apache.storm.tuple.Fields;

// TODO: for event time transparently handle "ts" field
public class Stream<T> {
    protected final StreamBuilder streamBuilder;
    protected final Node node;

    public Stream(StreamBuilder streamBuilder, Node node) {
        this.streamBuilder = streamBuilder;
        this.node = node;
    }

    public Stream<T> filter(Predicate<T> predicate) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new FilterProcessor<>(predicate), node.getOutputFields()));
    }

    public <R> Stream<R> map(Function<T, R> function) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new MapProcessor<>(function), new Fields("value")));
    }

    public <K, V> PairStream<K, V> mapToPair(PairFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder,
                addProcessorNode(new MapProcessor<>(function), new Fields("key", "value")));
    }

    public <R> Stream<R> flatMap(FlatMapFunction<T, R> function) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new FlatMapProcessor<>(function), new Fields("value")));
    }

    public <K, V> PairStream<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder,
                addProcessorNode(new FlatMapProcessor<>(function), new Fields("key", "value")));
    }

    // TODO: add window config
    public Stream<T> window(Window<?, ?> window) {
        return new Stream<>(streamBuilder,
                addNode(new WindowNode(window, UniqueIdGen.getInstance().getUniqueStreamId(), node.getOutputFields())));
    }

    // TODO: reduceByWindow
    //       transform ?
    //       sink/state

    public void forEach(Consumer<T> action) {
        addProcessorNode(new ForEachProcessor<>(action), new Fields());
    }

    public Stream<T> peek(Consumer<T> action) {
        return new Stream<>(
                streamBuilder,
                addProcessorNode(new PeekProcessor<>(action), new Fields("value")));
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

    Node getNode() {
        return node;
    }

    StreamBuilder getStreamBuilder() {
        return streamBuilder;
    }

    Node addNode(Node node) {
        return streamBuilder.addNode(this, node);
    }

    private Stream<T> global() {
        return new Stream<>(streamBuilder,
                addNode(new PartitionNode(node.getOutputStream(), node.getOutputFields(), GroupingInfo.global())));
    }

    Node addProcessorNode(Processor<?> processor, Fields outputFields) {
        return addNode(makeProcessorNode(processor, outputFields));
    }

    ProcessorNode makeProcessorNode(Processor<?> processor, Fields outputFields) {
        return new ProcessorNode(UniqueIdGen.getInstance().getUniqueStreamId(),
                processor, outputFields);
    }

    // TODO: repartition/parallelism

}
