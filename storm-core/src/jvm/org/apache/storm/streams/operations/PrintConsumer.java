package org.apache.storm.streams.operations;

/**
 * A consumer that prints the input
 *
 * @param <T> the value type
 */
public class PrintConsumer<T> implements Consumer<T> {
    @Override
    public void accept(T input) {
        System.out.println(input);
    }
}
