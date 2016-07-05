package org.apache.storm.streams;

import java.io.Serializable;

interface Processor<T> extends Serializable {
    void execute(T input, ProcessorContext context);
}
