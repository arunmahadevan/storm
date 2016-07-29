package org.apache.storm.streams.windowing;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import java.io.Serializable;

public interface Window<L, I> extends Serializable {

    L getWindowLength();

    I getSlidingInterval();

    String getTimestampField();

    String getLateTupleStream();

    Duration getLag();
}
