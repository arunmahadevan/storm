package org.apache.storm.streams;

class FlatMapProcessor<T, R> implements Processor<T> {
    private final Function<? super T, ? extends Iterable<? extends R>> function;

    FlatMapProcessor(Function<? super T, ? extends Iterable<? extends R>> function) {
        this.function = function;
    }

    @Override
    public void execute(T input, ProcessorContext context) {
        for(R res: function.apply(input)){
            context.forward(res);
        }
    }
}
