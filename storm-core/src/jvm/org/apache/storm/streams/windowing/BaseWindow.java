package org.apache.storm.streams.windowing;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

public abstract class BaseWindow<L, I> implements Window<L, I> {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BaseWindow<?, ?> that = (BaseWindow<?, ?>) o;

        if (timestampField != null ? !timestampField.equals(that.timestampField) : that.timestampField != null)
            return false;
        if (lateTupleStream != null ? !lateTupleStream.equals(that.lateTupleStream) : that.lateTupleStream != null)
            return false;
        return lag != null ? lag.equals(that.lag) : that.lag == null;

    }

    @Override
    public int hashCode() {
        int result = timestampField != null ? timestampField.hashCode() : 0;
        result = 31 * result + (lateTupleStream != null ? lateTupleStream.hashCode() : 0);
        result = 31 * result + (lag != null ? lag.hashCode() : 0);
        return result;
    }
}
