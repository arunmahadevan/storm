package org.apache.storm.streams;

public interface StreamBolt {
    void setTimestampField(String fieldName);
}
