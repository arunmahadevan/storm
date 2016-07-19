package org.apache.storm.streams;

import org.apache.storm.tuple.Fields;

import java.io.Serializable;

class GroupingInfo implements Serializable {
    enum Grouping {
        FIELDS, SHUFFLE, GLOBAL
    }

    private final Grouping grouping;
    private final Fields fields;

    private GroupingInfo(Grouping grouping, Fields fields) {
        this.grouping = grouping;
        this.fields = fields;
    }

    public static GroupingInfo shuffle() {
        return new GroupingInfo(Grouping.SHUFFLE, null);
    }

    public static GroupingInfo fields(Fields fields) {
        return new GroupingInfo(Grouping.FIELDS, fields);
    }

    public static GroupingInfo global() {
        return new GroupingInfo(Grouping.GLOBAL, null);
    }

    public Grouping getGrouping() {
        return grouping;
    }

    public Fields getFields() {
        return fields;
    }
}
