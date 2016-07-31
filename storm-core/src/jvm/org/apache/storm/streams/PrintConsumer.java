package org.apache.storm.streams;

public class PrintConsumer<T> implements Consumer<T> {
    @Override
    public void accept(T input) {
        System.out.println(input);
    }
}
