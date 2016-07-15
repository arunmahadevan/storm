package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

public class Stream<T> {
    private StreamBuilder streamBuilder;
    private Node node;

    public Stream(StreamBuilder topology, Node node) {
        this.streamBuilder = topology;
        this.node = node;
    }

    public Stream<T> filter(Predicate<? super T> predicate) {
        ProcessorNode filterNode = streamBuilder.addNode(this, makeProcessorNode(new FilterProcessor<>(predicate), node.getOutputFields()));
        return new Stream<>(streamBuilder, filterNode);
    }

    public <R> Stream<R> map(Function<? super T, ? extends R> function) {
        ProcessorNode mapNode = streamBuilder.addNode(this, makeProcessorNode(new MapProcessor<>(function), new Fields("value")));
        return new Stream<>(streamBuilder, mapNode);
    }

    public <R> Stream<R> flatMap(Function<? super T, ? extends Iterable<? extends R>> function) {
        ProcessorNode flatMapNode = streamBuilder.addNode(this, makeProcessorNode(new FlatMapProcessor<>(function), new Fields("value")));
        return new Stream<>(streamBuilder, flatMapNode);
    }

    public void forEach(Consumer<? super T> action) {
        streamBuilder.addNode(this, makeProcessorNode(new ForEachProcessor<>(action), new Fields()));
    }

    private ProcessorNode makeProcessorNode(Processor<?> processor, Fields outputFields) {
        return new ProcessorNode(UniqueIdGen.getInstance().getUniqueStreamId(),
                processor, outputFields);
    }

    GroupedStream<T> groupBy(Fields fields) {
        return null;
    }

    Node getNode() {
        return node;
    }
}
