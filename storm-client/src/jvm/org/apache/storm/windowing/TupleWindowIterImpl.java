package org.apache.storm.windowing;

import com.google.common.collect.Iterators;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * An iterator based implementation over the events in a window.
 */
public class TupleWindowIterImpl implements TupleWindow {
    private final Supplier<Iterator<Tuple>> tuplesIt;
    private final Supplier<Iterator<Tuple>> newTuplesIt;
    private final Supplier<Iterator<Tuple>> expiredTuplesIt;
    private final Long startTimestamp;
    private final Long endTimestamp;

    public TupleWindowIterImpl(Supplier<Iterator<Tuple>> tuplesIt,
                               Supplier<Iterator<Tuple>> newTuplesIt,
                               Supplier<Iterator<Tuple>> expiredTuplesIt,
                               Long startTimestamp, Long endTimestamp) {
        this.tuplesIt = tuplesIt;
        this.newTuplesIt = newTuplesIt;
        this.expiredTuplesIt = expiredTuplesIt;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    @Override
    public List<Tuple> get() {
        List<Tuple> tuples = new ArrayList<>();
        tuplesIt.get().forEachRemaining(t -> tuples.add(t));
        return tuples;
    }

    @Override
    public List<Tuple> getNew() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<Tuple> getExpired() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Iterator<Tuple> getIter() {
        return Iterators.unmodifiableIterator(tuplesIt.get());
    }

    @Override
    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public Long getStartTimestamp() {
        return startTimestamp;
    }
}
