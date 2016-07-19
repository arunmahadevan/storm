package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

public class PairStream<K, V> extends Stream<Pair<K, V>> {

    public PairStream(StreamBuilder topology, Node node) {
        super(topology, node);
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

    PairStream<K, V> partitionBy(Fields fields) {
        return new PairStream<>(streamBuilder, addNode(new PartitionNode(
                node.getOutputStream(), node.getOutputFields(), GroupingInfo.fields(fields))));
    }

    //TODO:
    // mapValues, flatMapValues
    // join, left, right etc

}
