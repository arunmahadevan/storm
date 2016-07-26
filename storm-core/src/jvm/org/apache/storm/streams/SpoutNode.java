package org.apache.storm.streams;

import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.Collections;
import java.util.Set;

class SpoutNode implements Node {
    private final IRichSpout spout;
    private final Fields outputFields;
    private String componentId;

    SpoutNode(IRichSpout spout) {
        this.spout = spout;
        this.outputFields = getDefaultOutputFields(spout);
    }

    private Fields getDefaultOutputFields(IRichSpout spout) {
        OutputFieldsGetter getter = new OutputFieldsGetter();
        spout.declareOutputFields(getter);
        return new Fields(getter.getFieldsDeclaration().get(Utils.DEFAULT_STREAM_ID).get_output_fields());
    }

    @Override
    public String getOutputStream() {
        // TODO: use componentid + streamid
        return Utils.DEFAULT_STREAM_ID;
    }

    @Override
    public Fields getOutputFields() {
        return outputFields;
    }

    IRichSpout getSpout() {
        return spout;
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }
}
