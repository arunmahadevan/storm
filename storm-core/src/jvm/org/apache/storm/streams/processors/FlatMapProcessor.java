package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.FlatMapFunction;

public class FlatMapProcessor<T, R> extends BaseProcessor<T> {
    private final FlatMapFunction<T, R> function;

    public FlatMapProcessor(FlatMapFunction<T, R> function) {
        this.function = function;
    }

    @Override
    public void execute(T input) {
        Iterable<R> it = function.apply(input);
        for(R res: it){
            context.forward(res);
        }
    }
}
