package org.apache.storm.streams;

import java.io.Serializable;

interface Processor<T> extends Serializable {
    void init(ProcessorContext context);

    void execute(T input);
}
