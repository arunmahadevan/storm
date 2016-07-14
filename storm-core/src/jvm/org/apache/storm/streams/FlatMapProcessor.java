package org.apache.storm.streams;

class FlatMapProcessor<T, R> extends BaseProcessor<T> {
    private final Function<? super T, ? extends Iterable<? extends R>> function;

    FlatMapProcessor(Function<? super T, ? extends Iterable<? extends R>> function) {
        this.function = function;
    }

    @Override
    public void execute(T input) {
        Iterable<? extends R> it = function.apply(input);
        for(R res: it){
            context.forward(res);
        }
    }
}
