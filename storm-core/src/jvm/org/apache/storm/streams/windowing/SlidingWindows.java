package org.apache.storm.streams.windowing;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * A sliding window specification based on a window length and sliding interval.
 *
 * @param <L> The type of the window length (e.g. Duration or Count)
 * @param <I> The type of the sliding interval (e.g. Duration or Count)
 */
public class SlidingWindows<L, I> extends BaseWindow<L, I> {
    private final L windowLength;
    private final I slidingInterval;

    private SlidingWindows(L windowLength, I slidingInterval) {
        this.windowLength = windowLength;
        this.slidingInterval = slidingInterval;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public L getWindowLength() {
        return windowLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public I getSlidingInterval() {
        return slidingInterval;
    }

    /**
     * Count based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public static SlidingWindows<Count, Count> of(Count windowLength, Count slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    /**
     * Time duration based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the time duration after which the window slides
     */
    public static SlidingWindows<Duration, Duration> of(Duration windowLength, Duration slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    /**
     * Tuple count and time duration based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the time duration after which the window slides
     */
    public static SlidingWindows<Count, Duration> of(Count windowLength, Duration slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    /**
     * Time duration and count based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public static SlidingWindows<Duration, Count> of(Duration windowLength, Count slidingInterval) {
        return new SlidingWindows<>(windowLength, slidingInterval);
    }

    /**
     * The name of the field in the tuple that contains the timestamp when the event occurred as a long value.
     * This is used of event-time based processing. If this config is set and the field is not present in the incoming tuple,
     * an {@link IllegalArgumentException} will be thrown.
     *
     * @param fieldName the name of the field that contains the timestamp
     */
    public SlidingWindows<L, I> withTimestampField(String fieldName) {
        timestampField = fieldName;
        return this;
    }

    /**
     * Specify a stream id on which late tuples are going to be emitted. They are going to be accessible via the
     * {@link org.apache.storm.topology.WindowedBoltExecutor#LATE_TUPLE_FIELD} field.
     * It must be defined on a per-component basis, and in conjunction with the
     * {@link BaseWindowedBolt#withTimestampField}, otherwise {@link IllegalArgumentException} will be thrown.
     *
     * @param streamId the name of the stream used to emit late tuples on
     */
    public SlidingWindows<L, I> withLateTupleStream(String streamId) {
        lateTupleStream = streamId;
        return this;
    }

    /**
     * Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps
     * cannot be out of order by more than this amount.
     *
     * @param duration the max lag duration
     */
    public SlidingWindows<L, I> withLag(Duration duration) {
        lag = duration;
        return this;
    }
}
