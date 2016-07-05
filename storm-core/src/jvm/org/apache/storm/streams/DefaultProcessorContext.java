package org.apache.storm.streams;

import java.io.Serializable;

public class DefaultProcessorContext implements ProcessorContext, Serializable {
    Object result = null;

    @Override
    public <T> void forward(T input) {
        result = input;
    }

    Object get() {
        Object tmp = result;
        result = null;
        return tmp;
    }
}
