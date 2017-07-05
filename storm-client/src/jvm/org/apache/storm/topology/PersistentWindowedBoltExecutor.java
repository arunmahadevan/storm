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
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
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

import static java.util.Collections.emptyIterator;

/**
 * Wraps a {@link IStatefulWindowedBolt} and handles the execution. Uses state and the underlying
 * checkpointing mechanisms to save the tuples in window to state. The tuples are also kept in-memory
 * by transparently caching the window partitions and checkpointing them as needed.
 */
public class PersistentWindowedBoltExecutor<T extends State> extends WindowedBoltExecutor implements IStatefulBolt<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentWindowedBoltExecutor.class);
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
        registrations.add(LinkedList.class.getName());
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
                 KeyValueState<String, Deque<Long>> partitionState,
                 KeyValueState<String, Object> windowSystemState) {
        init(topoConf, context, collector, windowState, partitionState, windowSystemState);
        doPrepare(topoConf, context, new NoAckOutputCollector(collector), state, true);
        Map<String, Object> wstate = new HashMap<>();
        windowSystemState.forEach(s -> wstate.put(s.getKey(), s.getValue()));
        restoreState(wstate);
    }

    @Override
    protected void start() {
        if (stateInitialized) {
            super.start();
        } else {
            LOG.debug("Will invoke start after state is initialized");
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!stateInitialized) {
            throw new IllegalStateException("execute invoked before initState with input tuple " + input);
        }
        super.execute(input);
        // StatefulBoltExecutor does the actual ack when the state is saved.
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
                 * NO-OP: the events are ack-ed in execute
                 */
            }

            @Override
            public void onActivation(Supplier<Iterator<Tuple>> eventsIt,
                                     Supplier<Iterator<Tuple>> newEventsIt,
                                     Supplier<Iterator<Tuple>> expiredIt,
                                     Long timestamp) {
                /*
                 * Here we don't set the tuples in windowedOutputCollector's context and emit un-anchored.
                 * The checkpoint tuple will trigger a checkpoint in the receiver with the emitted tuples.
                 */
                boltExecute(eventsIt, newEventsIt, expiredIt, timestamp);
            }
        };
    }

    private void init(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector,
                      KeyValueState<Long, WindowPartition<Tuple>> windowState,
                      KeyValueState<String, Deque<Long>> partitionState,
                      KeyValueState<String, Object> windowSystemState) {
        topologyContext = context;
        outputCollector = collector;
        state = new WindowState<>(windowState, partitionState, windowSystemState, this::getState,
            statefulWindowedBolt.maxEventsInMemory());
    }

    private KeyValueState<Long, WindowPartition<Tuple>> getWindowState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window";
        return (KeyValueState<Long, WindowPartition<Tuple>>) StateFactory.getState(namespace, topoConf, context);
    }

    private KeyValueState<String, Deque<Long>> getPartitionState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window-partitions";
        return (KeyValueState<String, Deque<Long>>) StateFactory.getState(namespace, topoConf, context);
    }

    private KeyValueState<String, Object> getWindowSystemState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window-systemstate";
        return (KeyValueState<String, Object>) StateFactory.getState(namespace, topoConf, context);
    }

    // a wrapper around the window related states that are checkpointed
    private static class WindowState<T> extends AbstractCollection<Event<T>> {
        // number of events per window-partition
        private static final int PARTITION_SZ = 1000;
        private static final int MIN_PARTITIONS = 10;
        private static final String PARTITION_IDS_KEY = "pk";
        private final KeyValueState<String, Deque<Long>> partitionIds;
        private final KeyValueState<Long, WindowPartition<T>> windowPartitions;
        private final KeyValueState<String, Object> windowSystemState;
        // ordered partition keys
        private Deque<Long> pids;
        private volatile long latestPartitionId;
        private LoadingCache<Long, WindowPartition<T>> cache;
        private Supplier<Map<String, ?>> windowSystemStateSupplier;
        private final ReentrantLock partitionIdsLock = new ReentrantLock(true);
        private final ReentrantLock windowPartitionsLock = new ReentrantLock(true);
        private final long maxEventsInMemory;

        WindowState(KeyValueState<Long, WindowPartition<T>> windowPartitions,
                    KeyValueState<String, Deque<Long>> partitionIds,
                    KeyValueState<String, Object> windowSystemState,
                    Supplier<Map<String, ?>> windowSystemStateSupplier,
                    long maxEventsInMemory) {
            this.windowPartitions = windowPartitions;
            this.partitionIds = partitionIds;
            this.windowSystemState = windowSystemState;
            this.windowSystemStateSupplier = windowSystemStateSupplier;
            this.maxEventsInMemory = Math.max(PARTITION_SZ * MIN_PARTITIONS, maxEventsInMemory);
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
                private Iterator<Long> ids = getIds();
                private Iterator<Event<T>> current = emptyIterator();
                private Iterator<Event<T>> removeFrom;
                private WindowPartition<T> curPartition;

                private Iterator<Long> getIds() {
                    try {
                        partitionIdsLock.lock();
                        LOG.debug("Iterator pids: {}", pids);
                        return new ArrayList<>(pids).iterator();
                    } finally {
                        partitionIdsLock.unlock();
                    }
                }

                @Override
                public void remove() {
                    if (removeFrom == null) {
                        throw new IllegalStateException("No calls to next() since last call to remove()");
                    }
                    removeFrom.remove();
                    removeFrom = null;
                    // Update access times or reload in case the partition was evicted while iterating
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
            partitionIds.prepareCommit(txid);
            windowPartitions.prepareCommit(txid);
            windowSystemState.prepareCommit(txid);
        }

        void commit(long txid) {
            partitionIds.commit(txid);
            windowPartitions.commit(txid);
            windowSystemState.commit(txid);
        }

        void rollback() {
            partitionIds.rollback();
            windowPartitions.rollback();
            windowSystemState.rollback();
        }

        private void initPartitions() {
            pids = partitionIds.get(PARTITION_IDS_KEY, new LinkedList<>());
            if (pids.isEmpty()) {
                pids.add(0L);
                partitionIds.put(PARTITION_IDS_KEY, pids);
            } else {
                latestPartitionId = pids.peekLast();
            }
        }

        private void initCache() {
            long size = maxEventsInMemory / PARTITION_SZ;
            LOG.info("maxEventsInMemory: {}, partition size: {}, number of partitions: {}",
                maxEventsInMemory, PARTITION_SZ, size);
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
                        try {
                            windowPartitionsLock.lock();
                            if (p.isEmpty() && pid != latestPartitionId) {
                                // if the empty partition was not invalidated by flush, but evicted from cache
                                if (notification.getCause() != RemovalCause.EXPLICIT) {
                                    deletePartition(pid);
                                    windowPartitions.delete(pid);
                                }
                            } else if (p.isModified()) {
                                windowPartitions.put(pid, p);
                            } else {
                                LOG.debug("WindowPartition '{}' is not modified", pid);
                            }
                        } finally {
                            windowPartitionsLock.unlock();
                        }
                    }
                }).build(new CacheLoader<Long, WindowPartition<T>>() {
                    @Override
                   public WindowPartition<T> load(Long id) throws Exception {
                                 LOG.debug("Load partition: {}", id);
                                 // load from state
                                 try {
                                     windowPartitionsLock.lock();
                                     return windowPartitions.get(id, new WindowPartition<>(id));
                                 } finally {
                                     windowPartitionsLock.unlock();
                                 }
                             }
                         }
                );
        }

        private void deletePartition(long pid) {
            LOG.debug("Delete partition: {}", pid);
            try {
                partitionIdsLock.lock();
                pids.remove(pid);
                partitionIds.put(PARTITION_IDS_KEY, pids);
            } finally {
                partitionIdsLock.unlock();
            }
        }

        private long getNextPartitionId() {
            try {
                partitionIdsLock.lock();
                pids.add(++latestPartitionId);
                partitionIds.put(PARTITION_IDS_KEY, pids);
            } finally {
                partitionIdsLock.unlock();
            }
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
                LOG.debug("Caching partition {}", partition);
                cache.put(partition.getId(), partition);
            }
        }

        private void flush() {
            LOG.debug("Flushing modified partitions");
            try {
                windowPartitionsLock.lock();
                cache.asMap().forEach((pid, p) -> {
                    if (p.isEmpty() && pid != latestPartitionId) {
                        LOG.debug("Invalidating empty partition {}", pid);
                        deletePartition(pid);
                        windowPartitions.delete(pid);
                        cache.invalidate(pid);
                    } else if (p.isModified()) {
                        LOG.debug("Updating modified partition {}", pid);
                        p.clearModified();
                        windowPartitions.put(pid, p);
                    }
                });
            } finally {
                windowPartitionsLock.unlock();
            }
            windowSystemStateSupplier.get().forEach(windowSystemState::put);
        }
    }

    // the window partition that holds the events
    public static class WindowPartition<T> implements Iterable<Event<T>> {
        private final ConcurrentLinkedQueue<Event<T>> events = new ConcurrentLinkedQueue<>();
        private final AtomicInteger size = new AtomicInteger();
        private final long id;
        private transient volatile boolean modified;

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

        int size() {
            return size.get();
        }

        long getId() {
            return id;
        }

        // for unit tests
        Collection<Event<T>> getEvents() {
            return Collections.unmodifiableCollection(events);
        }

        @Override
        public String toString() {
            return "WindowPartition{id=" + id + ", size=" + size + '}';
        }
    }

    /**
     * Creates an {@link OutputCollector} wrapper that ignores acks.
     * The {@link PersistentWindowedBoltExecutor} acks the tuples in execute and
     * this is to prevent double ack-ing
     */
    private static class NoAckOutputCollector extends OutputCollector {

        public NoAckOutputCollector(OutputCollector delegate) {
            super(delegate);
        }

        @Override
        public void ack(Tuple input) {
            // NOOP
        }
    }
}
