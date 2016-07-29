package org.apache.storm.streams.windowing;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

public abstract class BaseWindow<L, T> implements Window<L, T> {
    protected String timestampField;
    protected String lateTupleStream;
    protected Duration lag;

    @Override
    public String getTimestampField() {
        return timestampField;
    }

    @Override
    public String getLateTupleStream() {
        return lateTupleStream;
    }

    @Override
    public Duration getLag() {
        return lag;
    }
}
