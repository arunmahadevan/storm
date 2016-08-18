package org.apache.storm.streams.processors;

import org.apache.storm.state.KeyValueState;

/**
 * Top level interface for processors that want to do stateful processing
 */
public interface StatefulProcessor<K, V> {
    void initState(KeyValueState<K, V> keyValueState);
}
