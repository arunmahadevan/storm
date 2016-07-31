package org.apache.storm.streams;

import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.Collections;
import java.util.Set;

class SpoutNode extends Node {
    private final IRichSpout spout;

    SpoutNode(IRichSpout spout) {
        // TODO: use componentid + streamid for spout
        super(Utils.DEFAULT_STREAM_ID, getDefaultOutputFields(spout));
        this.spout = spout;
    }

    IRichSpout getSpout() {
        return spout;
    }

    private static Fields getDefaultOutputFields(IRichSpout spout) {
        OutputFieldsGetter getter = new OutputFieldsGetter();
        spout.declareOutputFields(getter);
        return new Fields(getter.getFieldsDeclaration().get(Utils.DEFAULT_STREAM_ID).get_output_fields());
    }

}
