/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.topology;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.Event;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyIterator;

/**
 * Wraps a {@link IStatefulWindowedBolt} and handles the execution. Saves the last expired
 * and evaluated states of the window during checkpoint and restores the state during recovery.
 */
public class StatefulWindowedBoltExecutor<T extends State> extends WindowedBoltExecutor implements IStatefulBolt<T> {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulWindowedBoltExecutor.class);
    private final IStatefulWindowedBolt<T> statefulWindowedBolt;
    private transient TopologyContext topologyContext;
    private transient OutputCollector outputCollector;
    private transient WindowState<Tuple> state;
    private transient boolean stateInitialized;
    private transient boolean prePrepared;

    public StatefulWindowedBoltExecutor(IStatefulWindowedBolt<T> bolt) {
        super(bolt);
        statefulWindowedBolt = bolt;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        prepare(topoConf, context, collector, getWindowState(topoConf, context), getPartitionState(topoConf, context));
    }

    // package access for unit tests
    void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector,
                 KeyValueState<Long, WindowPartition<Tuple>> windowState, KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState) {
        init(topoConf, context, collector, windowState, partitionState);
        doPrepare(topoConf, context, collector, state);
    }

    private void init(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector,
                      KeyValueState<Long, WindowPartition<Tuple>> windowState, KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState) {
        topologyContext = context;
        outputCollector = collector;
        state = new WindowState<>(windowState, partitionState);
    }

    @Override
    public void execute(Tuple input) {
        if (!stateInitialized) {
            throw new IllegalStateException("execute invoked before initState with input tuple " + input);
        }
        super.execute(input);
        // StatefulBoltExecutor handles the final ack when the state is saved
        outputCollector.ack(input);
    }

    @Override
    public void initState(T state) {
        if (stateInitialized) {
            LOG.warn("State is already initialized. Ignoring initState");
        } else {
            statefulWindowedBolt.initState(state);
            stateInitialized = true;
            start();
        }
    }

    @Override
    public void preCommit(long txid) {
        if (prePrepared) {
            LOG.debug("Commit streamState, txid {}", txid);
            state.commit(txid);
        } else {
            LOG.debug("Ignoring preCommit and not committing streamState.");
        }
    }

    @Override
    public void prePrepare(long txid) {
        if (stateInitialized) {
            LOG.debug("Prepare streamState, txid {}", txid);
            state.prepareCommit(txid);
            prePrepared = true;
        } else {
            LOG.warn("Cannot prepare before initState");
        }
    }

    @Override
    public void preRollback() {
        LOG.debug("Rollback streamState, stateInitialized {}", stateInitialized);
        state.rollback();
    }

    @Override
    protected WindowLifecycleListener<Tuple> newWindowLifecycleListener() {
        return new WindowLifecycleListener<Tuple>() {
            @Override
            public void onExpiry(List<Tuple> events) {
                /*
                 * NO-OP: the events should have been already acked in execute
                 * and removed from the state by WindowManager.scanEvents
                 */
            }

            @Override
            public void onActivation(List<Tuple> events, List<Tuple> newEvents, List<Tuple> expired, Long timestamp) {
                /*
                 * Here we emit without anchoring since the tuples in window are acked after the
                 * checkpoint barrier is received and the window state is check-pointed.
                 */
                boltExecute(events, newEvents, expired, timestamp);
            }
        };
    }

    private KeyValueState<Long, WindowPartition<Tuple>> getWindowState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window";
        return (KeyValueState<Long, WindowPartition<Tuple>>) StateFactory.getState(namespace, topoConf, context);
    }

    private KeyValueState<String, ConcurrentLinkedDeque<Long>> getPartitionState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window-partitions";
        return (KeyValueState<String, ConcurrentLinkedDeque<Long>>) StateFactory.getState(namespace, topoConf, context);
    }

    private static class WindowState<T> extends AbstractCollection<Event<T>> {
        private static final int PARTITION_SZ = 1000;
        private static final String PARTITION_KEY = "partition-key";
        private final KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState;
        private final KeyValueState<Long, WindowPartition<T>> keyValueState;
        private ConcurrentLinkedDeque<Long> partitionIds;
        private long curId;
        private WindowPartition<T> curPartition;
        private final Set<WindowPartition<T>> modified = Collections.newSetFromMap(new ConcurrentHashMap<>());

        WindowState(KeyValueState<Long, WindowPartition<T>> keyValueState, KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState) {
            this.keyValueState = keyValueState;
            this.partitionState = partitionState;
            initCurPartitions();
        }

        private void initCurPartitions() {
            partitionIds = partitionState.get(PARTITION_KEY, new ConcurrentLinkedDeque<>());
            if (partitionIds.isEmpty()) {
                partitionIds.add(curId);
                partitionState.put(PARTITION_KEY, partitionIds);
            } else {
                curId = partitionIds.peekLast();
            }
            curPartition = keyValueState.get(curId, new WindowPartition<>(curId));
        }

        @Override
        public Iterator<Event<T>> iterator() {

            return new Iterator<Event<T>>() {
                private Iterator<Long> ids = partitionIds.iterator();
                private Iterator<Event<T>> current = emptyIterator();
                private Iterator<Event<T>> removeFrom;
                private long curIterId;
                private WindowPartition<T> curIterPartition;

                @Override
                public void remove() {
                    if (removeFrom == null) {
                        throw new IllegalStateException("No calls to next() since last call to remove()");
                    }
                    removeFrom.remove();
                    removeFrom = null;
                    modified.add(curIterPartition);
                }

                @Override
                public boolean hasNext() {
                    boolean curHasNext = current.hasNext();
                    while (!curHasNext && ids.hasNext()) {
                        curIterId = ids.next();
                        curIterPartition = keyValueState.get(curIterId);
                        if (curIterPartition != null) {
                            current = curIterPartition.iterator();
                            curHasNext = current.hasNext();
                        }
                    }
                    return curHasNext;
                }

                @Override
                public Event<T> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    removeFrom = current;
                    return current.next();
                }
            };
        }

        @Override
        public boolean add(Event<T> event) {
            if (curPartition.size() >= PARTITION_SZ) {
                keyValueState.put(curId, curPartition);
                curId++;
                curPartition = new WindowPartition<>(curId);
                partitionIds.add(curId);
                partitionState.put(PARTITION_KEY, partitionIds);
            }
            curPartition.add(event);
            return true;
        }

        private void flush() {
            Iterator<WindowPartition<T>> it = modified.iterator();
            while (it.hasNext()) {
                WindowPartition<T> p = it.next();
                it.remove();
                if (p.isEmpty()) {
                    keyValueState.delete(p.getId());
                    partitionIds.remove(p.getId());
                } else {
                    keyValueState.put(p.getId(), p);
                }
            }
            partitionState.put(PARTITION_KEY, partitionIds);
            keyValueState.put(curId, curPartition);
        }

        void prepareCommit(long txid) {
            flush();
            partitionState.prepareCommit(txid);
            keyValueState.prepareCommit(txid);
        }

        void commit(long txid) {
            flush();
            partitionState.commit(txid);
            keyValueState.commit(txid);
        }

        void rollback() {
            flush();
            partitionState.rollback();
            keyValueState.rollback();
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }
    }

    public static class WindowPartition<T> implements Iterable<Event<T>> {
        private final ConcurrentLinkedQueue<Event<T>> events = new ConcurrentLinkedQueue<>();
        private final AtomicInteger size = new AtomicInteger();
        private final long id;

        public WindowPartition(long id) {
            this.id = id;
        }

        void add(Event<T> event) {
            events.add(event);
            size.incrementAndGet();
        }

        boolean isEmpty() {
            return events.isEmpty();
        }

        @Override
        public Iterator<Event<T>> iterator() {
            return new Iterator<Event<T>>() {
                Iterator<Event<T>> it = events.iterator();
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Event<T> next() {
                    return it.next();
                }

                @Override
                public void remove() {
                    it.remove();
                    size.decrementAndGet();
                }
            };
        }

        public int size() {
            return size.get();
        }

        public long getId() {
            return id;
        }
    }

}
