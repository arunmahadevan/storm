/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.Event;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.WaterMarkEvent;
import org.apache.storm.windowing.WaterMarkEventGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.mockito.AdditionalAnswers.returnsArgAt;

/**
 * Unit tests for {@link PersistentWindowedBoltExecutor}
 */
@RunWith(MockitoJUnitRunner.class)
public class PersistentWindowedBoltExecutorTest {
    public static final String LATE_STREAM = "late_stream";
    private PersistentWindowedBoltExecutor<KeyValueState<String, String>> executor;
    private IStatefulWindowedBolt<KeyValueState<String, String>> mockBolt;
    private OutputCollector mockOutputCollector;
    private TopologyContext mockTopologyContext;
    private Map<String, Object> mockStormConf = new HashMap<>();
    private TimestampExtractor mockTimestampExtractor;
    private WaterMarkEventGenerator mockWaterMarkEventGenerator;
    private KeyValueState mockPartitionState;
    private KeyValueState mockWindowState;
    private KeyValueState mockSystemState;

    private static final String PARTITION_KEY = "pk";
    private static final String EVICTION_STATE_KEY = "es";
    private static final String TRIGGER_STATE_KEY = "ts";
    private long tupleTs;

    @Captor
    private ArgumentCaptor<Tuple> tupleCaptor;
    @Captor
    private ArgumentCaptor<Collection<Tuple>> anchorCaptor;
    @Captor
    private ArgumentCaptor<String> stringCaptor;
    @Captor
    private ArgumentCaptor<Long> longCaptor;
    @Captor
    private ArgumentCaptor<Values> valuesCaptor;
    @Captor
    private ArgumentCaptor<TupleWindow> tupleWindowCaptor;
    @Captor
    private ArgumentCaptor<Deque<Long>> partitionValuesCaptor;
    @Captor
    private ArgumentCaptor<PersistentWindowedBoltExecutor.WindowPartition<Event<Tuple>>> windowValuesCaptor;
    @Captor
    private ArgumentCaptor<Object> systemValuesCaptor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mockBolt = Mockito.mock(IStatefulWindowedBolt.class);
        mockWaterMarkEventGenerator = Mockito.mock(WaterMarkEventGenerator.class);
        mockTimestampExtractor = Mockito.mock(TimestampExtractor.class);
        tupleTs = System.currentTimeMillis();
        Mockito.when(mockTimestampExtractor.extractTimestamp(Mockito.any())).thenReturn(tupleTs);
        Mockito.when(mockBolt.getTimestampExtractor()).thenReturn(mockTimestampExtractor);
        mockPartitionState = Mockito.mock(KeyValueState.class);
        mockWindowState = Mockito.mock(KeyValueState.class);
        mockSystemState = Mockito.mock(KeyValueState.class);
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        Mockito.when(mockTopologyContext.getThisStreams()).thenReturn(Collections.singleton(LATE_STREAM));
        mockOutputCollector = Mockito.mock(OutputCollector.class);
        executor = new PersistentWindowedBoltExecutor<>(mockBolt);
        mockStormConf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 5);
        mockStormConf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, 5);
        mockStormConf.put(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, LATE_STREAM);
        mockStormConf.put(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 100_000);
        Mockito.when(mockPartitionState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(mockWindowState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(mockSystemState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        executor.prepare(mockStormConf, mockTopologyContext, mockOutputCollector,
            mockWindowState, mockPartitionState, mockSystemState);
    }

    @Test
    public void testExecuteTuple() throws Exception {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(true);
        Tuple mockTuple = Mockito.mock(Tuple.class);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;
        executor.execute(mockTuple);
        // should be ack-ed once
        Mockito.verify(mockOutputCollector, Mockito.times(1)).ack(mockTuple);
    }

    @Test
    public void testExecuteLatetuple() throws Exception {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(false);
        Tuple mockTuple = Mockito.mock(Tuple.class);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;
        executor.execute(mockTuple);
        // ack-ed once
        Mockito.verify(mockOutputCollector, Mockito.times(1)).ack(mockTuple);
        // late tuple emitted
        Mockito.verify(mockOutputCollector, Mockito.times(1))
            .emit(stringCaptor.capture(), anchorCaptor.capture(), valuesCaptor.capture());
        Assert.assertEquals(LATE_STREAM, stringCaptor.getValue());
        Assert.assertEquals(Collections.singletonList(mockTuple), anchorCaptor.getValue());
        Assert.assertEquals(new Values(mockTuple), valuesCaptor.getValue());
    }

    @Test
    public void testActivation() throws Exception {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(true);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;

        List<Tuple> mockTuples = getMockTuples(5);
        mockTuples.forEach(t -> executor.execute(t));
        // all tuples acked
        Mockito.verify(mockOutputCollector, Mockito.times(5)).ack(tupleCaptor.capture());
        Assert.assertArrayEquals(mockTuples.toArray(), tupleCaptor.getAllValues().toArray());

        // trigger the window
        long activationTs = tupleTs + 1000;
        executor.getWindowManager().add(new WaterMarkEvent<>(activationTs));

        // iterate the tuples
        Mockito.verify(mockBolt, Mockito.times(1)).execute(tupleWindowCaptor.capture());
        Assert.assertEquals(5, tupleWindowCaptor.getValue().get().size());
        // iterating multiple times should produce same events
        Assert.assertEquals(5, tupleWindowCaptor.getValue().get().size());
        Assert.assertEquals(5, tupleWindowCaptor.getValue().get().size());

        executor.prePrepare(0);

        // partition ids
        Mockito.verify(mockPartitionState, Mockito.times(1)).put(stringCaptor.capture(), partitionValuesCaptor.capture());
        Assert.assertEquals(PARTITION_KEY, stringCaptor.getAllValues().get(0));
        Assert.assertEquals(Collections.singletonList(0L), partitionValuesCaptor.getAllValues().get(0));

        // window partitions
        Mockito.verify(mockWindowState, Mockito.times(1)).put(longCaptor.capture(), windowValuesCaptor.capture());
        Assert.assertEquals(0L, (long) longCaptor.getAllValues().get(0));
        Assert.assertEquals(5, windowValuesCaptor.getAllValues().get(0).size());
        List<Event<?>> tuples = windowValuesCaptor.getAllValues().get(0)
            .getEvents().stream().map(Event::get).collect(Collectors.toList());
        Assert.assertArrayEquals(mockTuples.toArray(), tuples.toArray());

        // window system state
        Mockito.verify(mockSystemState, Mockito.times(2)).put(stringCaptor.capture(), systemValuesCaptor.capture());
        Assert.assertEquals(EVICTION_STATE_KEY, stringCaptor.getAllValues().get(1));
        Assert.assertEquals(Optional.of(Pair.of(5L, 5L)), systemValuesCaptor.getAllValues().get(0));
        Assert.assertEquals(TRIGGER_STATE_KEY, stringCaptor.getAllValues().get(2));
        Assert.assertEquals(Optional.of(tupleTs), systemValuesCaptor.getAllValues().get(1));
    }

    @Test
    public void testCacheEviction() {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(true);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;
        List<Tuple> mockTuples = getMockTuples(20000);
        mockTuples.forEach(t -> executor.execute(t));

        Mockito.verify(mockWindowState, Mockito.times(10)).put(longCaptor.capture(), windowValuesCaptor.capture());
        Assert.assertEquals(10, longCaptor.getAllValues().size());
        Assert.assertEquals(10, windowValuesCaptor.getAllValues().size());
        // number of evicted events
        Assert.assertEquals(10_000, windowValuesCaptor.getAllValues().stream()
            .mapToInt(x -> x.size()).sum());

        Map<Long, PersistentWindowedBoltExecutor.WindowPartition<Event<Tuple>>> partitionMap = new HashMap<>();
        windowValuesCaptor.getAllValues().forEach(v -> partitionMap.put(v.getId(), v));

        Mockito.verify(mockPartitionState, Mockito.times(20)).put(stringCaptor.capture(), partitionValuesCaptor.capture());
        Assert.assertEquals(LongStream.range(0, 20).boxed().collect(Collectors.toList()), partitionValuesCaptor.getAllValues().get(19));

        Mockito.when(mockWindowState.get(Mockito.any(), Mockito.any())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                PersistentWindowedBoltExecutor.WindowPartition<Event<Tuple>> evicted = partitionMap.get(args[0]);
                return evicted != null ? evicted : args[1];
            }
        });

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                partitionMap.put((long)args[0], (PersistentWindowedBoltExecutor.WindowPartition<Event<Tuple>>)args[1]);
                return null;
            }
        }).when(mockWindowState).put(Mockito.any(), Mockito.any());

        // trigger the window
        long activationTs = tupleTs + 1000;
        executor.getWindowManager().add(new WaterMarkEvent<>(activationTs));

        Mockito.verify(mockBolt, Mockito.times(4000)).execute(tupleWindowCaptor.capture());
    }

    private List<Tuple> getMockTuples(int count) {
        List<Tuple> tuples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            tuples.add(Mockito.mock(Tuple.class));
        }
        return tuples;
    }
}