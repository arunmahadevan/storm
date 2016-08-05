package org.apache.storm.streams;

import org.apache.storm.generated.StreamInfo;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Bolt node holds IRich or IBasic bolts that are passed
 * via the {@code Stream#to()} api.
 */
public class BoltNode extends Node {
    private final IComponent bolt;

    public BoltNode(IComponent bolt) {
        super(Utils.DEFAULT_STREAM_ID, getDefaultOutputFields(bolt, Utils.DEFAULT_STREAM_ID));
        if (bolt instanceof IRichBolt || bolt instanceof IBasicBolt) {
            this.bolt = bolt;
        } else {
            throw new IllegalArgumentException("Should be an IRichBolt or IBasicBolt");
        }
    }

    public IComponent getBolt() {
        return bolt;
    }
}
