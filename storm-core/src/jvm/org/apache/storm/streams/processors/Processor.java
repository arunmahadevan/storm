package org.apache.storm.streams.processors;

import java.io.Serializable;

/**
 * A processor processes a stream of elements and produces some result.
 *
 * @param <T> the type of the input that is processed
 */
public interface Processor<T> extends Serializable {
    /**
     * Initializes the processor. This is typically invoked from the underlying
     * storm bolt's prepare method.
     *
     * @param context the processor context
     */
    void init(ProcessorContext context);

    /**
     * Executes some operation on the input and possibly emits some result.
     *
     * @param input    the input to be processed
     * @param streamId the source stream id from where the input is received
     */
    void execute(T input, String streamId);

    /**
     * Punctuation marks end of a batch which can be used to compute and pass
     * the results of one stage in the pipeline to the next. For e.g. emit the results of an aggregation.
     *
     * @param stream the stream id on which the punctuation arrived
     */
    void punctuate(String stream);
}
