package org.apache.storm.streams.windowing;

import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

public class SlidingWindows<L, I> extends BaseWindow<L, I> {
    private final L windowLength;
    private final I slidingInterval;

    private SlidingWindows(L windowLength, I slidingInterval) {
        this.windowLength = windowLength;
        this.slidingInterval = slidingInterval;
    }

    @Override
    public L getWindowLength() {
        return windowLength;
    }

    @Override
    public I getSlidingInterval() {
        return slidingInterval;
    }

    public static SlidingWindows<Count, Count> of(Count windowLength, Count slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    public static SlidingWindows<Duration, Duration> of(Duration windowLength, Duration slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    public static SlidingWindows<Count, Duration> of(Count windowLength, Duration slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    public static SlidingWindows<Duration, Count> of(Duration windowLength, Count slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    public SlidingWindows<L, I> withTimestampField(String fieldName) {
        timestampField = fieldName;
        return this;
    }

    public SlidingWindows<L, I> withLateTupleStream(String streamId) {
        lateTupleStream = streamId;
        return this;
    }

    public SlidingWindows<L, I> withLag(Duration duration) {
        lag = duration;
        return this;
    }
}
