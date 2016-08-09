package org.apache.storm.streams.windowing;

import org.apache.storm.topology.base.BaseWindowedBolt;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;
import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * A tumbling window specification. The window tumbles after the specified window length.
 *
 * @param <L> the type of the length of the window (e.g Count, Duration)
 */
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

    /**
     * A count based tumbling window.
     *
     * @param count the number of tuples after which the window tumbles
     */
    public static TumblingWindows<Count> of(Count count) {
        return new TumblingWindows<>(count);
    }

    /**
     * A time duration based tumbling window.
     *
     * @param duration the time duration after which the window tumbles
     */
    public static TumblingWindows<Duration> of(Duration duration) {
        return new TumblingWindows<>(duration);
    }

    /**
     * The name of the field in the tuple that contains the timestamp when the event occurred as a long value.
     * This is used of event-time based processing. If this config is set and the field is not present in the incoming tuple,
     * an {@link IllegalArgumentException} will be thrown.
     *
     * @param fieldName the name of the field that contains the timestamp
     */
    public TumblingWindows<L> withTimestampField(String fieldName) {
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
    public TumblingWindows<L> withLateTupleStream(String streamId) {
        lateTupleStream = streamId;
        return this;
    }

    /**
     * Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps
     * cannot be out of order by more than this amount.
     *
     * @param duration the max lag duration
     */
    public TumblingWindows<L> withLag(Duration duration) {
        lag = duration;
        return this;
    }

}
