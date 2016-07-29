package org.apache.storm.streams.windowing;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;
import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

public class TumblingWindows<L> extends BaseWindow<L, L> {
    private final L windowLength;

    private TumblingWindows(L windowLength) {
        this.windowLength = windowLength;
    }

    @Override
    public L getWindowLength() {
        return windowLength;
    }

    @Override
    public L getSlidingInterval() {
        return windowLength;
    }

    public static TumblingWindows<Count> of(Count windowLength) {
        return new TumblingWindows<>(windowLength);
    }

    public static TumblingWindows<Duration> of(Duration windowLength) {
        return new TumblingWindows<>(windowLength);
    }

    public TumblingWindows<L> withTimestampField(String fieldName) {
        timestampField = fieldName;
        return this;
    }

    public TumblingWindows<L> withLateTupleStream(String streamId) {
        lateTupleStream = streamId;
        return this;
    }

    public TumblingWindows<L> withLag(Duration duration) {
        lag = duration;
        return this;
    }

}
