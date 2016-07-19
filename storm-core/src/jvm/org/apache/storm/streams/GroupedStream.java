package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

public class GroupedStream<T> {
    private final Stream<T> stream;
    private final Fields groupedFields;

    GroupedStream(Stream<T> stream, Fields groupedFields) {
        this.stream = stream;
        this.groupedFields = groupedFields;
    }

    public <R> Stream<Pair<T, R>> aggregate(Aggregator<T, R> aggregator) {
//        Stream<T> partitionBy = stream.partitionBy(groupedFields);
//        StreamBuilder streamBuilder = stream.getStreamBuilder();
//        Node node = streamBuilder.addNode(stream, stream.makeProcessorNode(new GroupedAggregateProcessor<>(aggregator), new Fields("value"),
//                GroupingInfo.fields(groupedFields)));
//        return new Stream<>(streamBuilder, node);
        return null;
    }
}
