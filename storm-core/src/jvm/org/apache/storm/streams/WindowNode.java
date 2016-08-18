package org.apache.storm.streams;

import org.apache.storm.streams.windowing.Window;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;

public class WindowNode extends Node {
    private final Window<?, ?> windowParams;

    public static final String PUNCTUATION = "__punctuation";

    WindowNode(Window<?, ?> windowParams, String outputStream, Fields outputFields) {
        super(outputStream, outputFields);
        this.windowParams = windowParams;
    }

    Window<?, ?> getWindowParams() {
        return windowParams;
    }

}
