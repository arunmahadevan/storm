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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.storm.Config;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.Event;
import org.apache.storm.windowing.EventImpl;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static java.util.Collections.emptyIterator;

/**
 * Wraps a {@link IStatefulWindowedBolt} and handles the execution. Uses state and the underlying
 * checkpointing mechanisms to save the tuples in window to state. The tuples are also kept in-memory
 * by transparently caching the window partitions and checkpointing them as needed.
 */
public class PersistentWindowedBoltExecutor<T extends State> extends WindowedBoltExecutor implements IStatefulBolt<T> {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulWindowedBoltExecutor.class);
    private final IStatefulWindowedBolt<T> statefulWindowedBolt;
    private transient TopologyContext topologyContext;
    private transient OutputCollector outputCollector;
    private transient WindowState<Tuple> state;
    private transient boolean stateInitialized;
    private transient boolean prePrepared;

    public PersistentWindowedBoltExecutor(IStatefulWindowedBolt<T> bolt) {
        super(bolt);
        statefulWindowedBolt = bolt;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        List<String> registrations = (List<String>) topoConf.getOrDefault(Config.TOPOLOGY_STATE_KRYO_REGISTER, new ArrayList<>());
        registrations.add(ConcurrentLinkedQueue.class.getName());
        registrations.add(ConcurrentLinkedDeque.class.getName());
        registrations.add(AtomicInteger.class.getName());
        registrations.add(EventImpl.class.getName());
        registrations.add(WindowPartition.class.getName());
        topoConf.put(Config.TOPOLOGY_STATE_KRYO_REGISTER, registrations);
        prepare(topoConf, context, collector,
            getWindowState(topoConf, context),
            getPartitionState(topoConf, context),
            getWindowSystemState(topoConf, context));
    }

    // package access for unit tests
    void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector,
                 KeyValueState<Long, WindowPartition<Tuple>> windowState,
                 KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState,
                 KeyValueState<String, Object> windowSystemState) {
        init(topoConf, context, collector, windowState, partitionState, windowSystemState);
        doPrepare(topoConf, context, collector, state, true);
        Map<String, Object> wstate = new HashMap<>();
        windowSystemState.forEach(s -> wstate.put(s.getKey(), s.getValue()));
        restoreState(wstate);
    }

    @Override
    protected void start() {
        if (!stateInitialized) {
            LOG.debug("Will invoke start after state is initialized");
        } else {
            super.start();
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!stateInitialized) {
            throw new IllegalStateException("execute invoked before initState with input tuple " + input);
        }
        super.execute(input);
        // StatefulBoltExecutor handles the final ack when the state is saved
        // TODO: multiple ack for same input OK? since late tuples are acked in WBE
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
    public void preCommit(long txid) {
        if (prePrepared) {
            LOG.debug("Commit streamState, txid {}", txid);
            state.commit(txid);
        } else {
            LOG.debug("Ignoring preCommit and not committing streamState.");
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
            public void onActivation(Supplier<Iterator<Tuple>> eventsIt,
                                     Supplier<Iterator<Tuple>> newEventsIt,
                                     Supplier<Iterator<Tuple>> expiredIt,
                                     Long timestamp) {
                /*
                 * Here we don't set the tuples in windowedOutputCollector's context and emit un-anchored.
                 * The tuples in window are acked during commit. The assumption is a reliable FIFO TCP channel between the
                 * workers. The checkpoint barrier will trigger a checkpoint in the receiver with the emitted tuple.
                 */
                boltExecute(eventsIt, newEventsIt, expiredIt, timestamp);
            }
        };
    }

    private void init(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector,
                      KeyValueState<Long, WindowPartition<Tuple>> windowState,
                      KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState,
                      KeyValueState<String, Object> windowSystemState) {
        topologyContext = context;
        outputCollector = collector;
        state = new WindowState<>(windowState, partitionState, windowSystemState, () -> getState(),
            statefulWindowedBolt.maxEventsInMemory());
    }

    private KeyValueState<Long, WindowPartition<Tuple>> getWindowState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window";
        return (KeyValueState<Long, WindowPartition<Tuple>>) StateFactory.getState(namespace, topoConf, context);
    }

    private KeyValueState<String, ConcurrentLinkedDeque<Long>> getPartitionState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window-partitions";
        return (KeyValueState<String, ConcurrentLinkedDeque<Long>>) StateFactory.getState(namespace, topoConf, context);
    }

    private KeyValueState<String, Object> getWindowSystemState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window-systemstate";
        return (KeyValueState<String, Object>) StateFactory.getState(namespace, topoConf, context);
    }

    private static class WindowState<T> extends AbstractCollection<Event<T>> {
        private static final int PARTITION_SZ = 1000;
        private static final String PARTITION_KEY = "pk";
        private final KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState;
        private final KeyValueState<Long, WindowPartition<T>> keyValueState;
        private final KeyValueState<String, Object> windowSystemState;
        private ConcurrentLinkedDeque<Long> partitionIds;
        private volatile long latestPartitionId;
        private LoadingCache<Long, WindowPartition<T>> cache;
        private Supplier<Map<String, ?>> windowSystemStateSupplier;
        private final ReentrantLock partitionLock = new ReentrantLock(true);
        private final long maxEventsInMemory;

        WindowState(KeyValueState<Long, WindowPartition<T>> keyValueState,
                    KeyValueState<String, ConcurrentLinkedDeque<Long>> partitionState,
                    KeyValueState<String, Object> windowSystemState,
                    Supplier<Map<String, ?>> windowSystemStateSupplier,
                    long maxEventsInMemory) {
            this.keyValueState = keyValueState;
            this.partitionState = partitionState;
            this.windowSystemState = windowSystemState;
            this.windowSystemStateSupplier = windowSystemStateSupplier;
            this.maxEventsInMemory = Math.max(10_000, maxEventsInMemory);
            initPartitions();
            initCache();
        }

        @Override
        public boolean add(Event<T> event) {
            WindowPartition<T> p = getCurPartition();
            if (p.size() >= PARTITION_SZ) {
                p = getNextPartition();
            }
            p.add(event);
            return true;
        }

        @Override
        public Iterator<Event<T>> iterator() {

            return new Iterator<Event<T>>() {
                private Iterator<Long> ids = partitionIds.iterator();
                private Iterator<Event<T>> current = emptyIterator();
                private Iterator<Event<T>> removeFrom;
                private WindowPartition<T> curPartition;

                @Override
                public void remove() {
                    if (removeFrom == null) {
                        throw new IllegalStateException("No calls to next() since last call to remove()");
                    }
                    removeFrom.remove();
                    removeFrom = null;
                    // Reload in case the current partition was evicted while iterating
                    mayBeCachePartition(curPartition);
                }

                @Override
                public boolean hasNext() {
                    boolean curHasNext = current.hasNext();
                    while (!curHasNext && ids.hasNext()) {
                        curPartition = getPartition(ids.next());
                        if (curPartition != null) {
                            current = curPartition.iterator();
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
        public int size() {
            throw new UnsupportedOperationException();
        }

        void prepareCommit(long txid) {
            flush();
            partitionState.prepareCommit(txid);
            keyValueState.prepareCommit(txid);
            windowSystemState.prepareCommit(txid);
        }

        void commit(long txid) {
            flush();
            partitionState.commit(txid);
            keyValueState.commit(txid);
            windowSystemState.commit(txid);
        }

        void rollback() {
            partitionState.rollback();
            keyValueState.rollback();
            windowSystemState.rollback();
        }

        private void initPartitions() {
            partitionIds = partitionState.get(PARTITION_KEY, new ConcurrentLinkedDeque<>());
            if (partitionIds.isEmpty()) {
                partitionIds.add(0L);
                partitionState.put(PARTITION_KEY, partitionIds);
            } else {
                latestPartitionId = partitionIds.peekLast();
            }
        }

        private void initCache() {
            long size = maxEventsInMemory / PARTITION_SZ;
            cache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .removalListener(new RemovalListener<Long, WindowPartition<T>>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, WindowPartition<T>> notification) {
                        LOG.debug("onRemoval for id '{}', WindowPartition '{}'", notification.getKey(), notification.getValue());
                        Long pid = notification.getKey();
                        WindowPartition<T> p = notification.getValue();
                        if (pid == null || p == null) {
                            return;
                        }
                        if (!p.isModified()) {
                            LOG.debug("WindowPartition '{}' is not modified", p);
                        } else if (p.isEmpty() && pid != latestPartitionId) {
                            deletePartition(pid);
                        } else {
                            keyValueState.put(pid, p);
                        }
                    }
                }).build(new CacheLoader<Long, WindowPartition<T>>() {
                             @Override
                             public WindowPartition<T> load(Long id) throws Exception {
                                 LOG.debug("Load partition: {}", id);
                                 // load from state
                                 return keyValueState.get(id, new WindowPartition<>(id));
                             }
                         }
                );

        }

        private void updatePartitionState() {
            try {
                partitionLock.lock();
                partitionState.put(PARTITION_KEY, partitionIds);
            } finally {
                partitionLock.unlock();
            }
        }

        private void deletePartition(long pid) {
            LOG.debug("deletePartition: {}", pid);
            keyValueState.delete(pid);
            partitionIds.remove(pid);
            updatePartitionState();
        }

        private long getNextPartitionId() {
            partitionIds.add(++latestPartitionId);
            updatePartitionState();
            return latestPartitionId;
        }

        private WindowPartition<T> getNextPartition() {
            return getPartition(getNextPartitionId());
        }

        private WindowPartition<T> getCurPartition() {
            return getPartition(latestPartitionId);
        }

        private WindowPartition<T> getPartition(long id) {
            try {
                return cache.get(id);
            } catch (ExecutionException ex) {
                LOG.error("Got error: ", ex);
                throw new RuntimeException(ex);
            }
        }

        private void mayBeCachePartition(WindowPartition<T> partition) {
            if (cache.getIfPresent(partition.getId()) == null) {
                cache.put(partition.getId(), partition);
            }
        }

        private void flush() {
            cache.asMap().forEach((pid, p) -> {
                if (p.isModified()) {
                    if (p.isEmpty() && pid != latestPartitionId) {
                        cache.invalidate(pid);
                    } else {
                        p.clearModified();
                        // TODO: lock?
                        keyValueState.put(pid, p);
                    }
                }
            });
            windowSystemStateSupplier.get().forEach(windowSystemState::put);
        }
    }

    public static class WindowPartition<T> implements Iterable<Event<T>> {
        private final ConcurrentLinkedQueue<Event<T>> events = new ConcurrentLinkedQueue<>();
        private final AtomicInteger size = new AtomicInteger();
        private final long id;
        private transient volatile boolean modified = false;

        public WindowPartition(long id) {
            this.id = id;
        }

        void add(Event<T> event) {
            events.add(event);
            size.incrementAndGet();
            setModified();
        }

        boolean isModified() {
            return modified;
        }

        void setModified() {
            if (!modified) {
                modified = true;
            }
        }

        void clearModified() {
            if (modified) {
                modified = false;
            }
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
                    setModified();
                }
            };
        }

        public int size() {
            return size.get();
        }

        public long getId() {
            return id;
        }

        @Override
        public String toString() {
            return "WindowPartition{events=" + events + ", size=" + size + ", id=" + id + '}';
        }
    }
}
