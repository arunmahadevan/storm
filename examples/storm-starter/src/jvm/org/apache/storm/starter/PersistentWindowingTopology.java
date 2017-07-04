/*
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

package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * An example that demonstrates the usage of {@link org.apache.storm.topology.IStatefulWindowedBolt} with window
 * persistence.
 * <p>
 * The framework automatically checkpoints the tuples in window along with the bolt's state and restores the same
 * during restarts.
 * </p>
 */
public class PersistentWindowingTopology {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentWindowingTopology.class);

    private static class Averages {
        private final double global;
        private final double window;

        Averages(double global, double window) {
            this.global = global;
            this.window = window;
        }

        @Override
        public String toString() {
            return "Averages{" + "global=" + String.format("%.2f", global) + ", window=" + String.format("%.2f", window) + '}';
        }
    }

    /**
     * A bolt that uses stateful persistence to store the windows along with the state.
     */
    private static class AvgBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Pair<Long, Long>>> {
        private static final String STATE_KEY = "avg";

        private OutputCollector collector;
        private KeyValueState<String, Pair<Long, Long>> state;
        private Pair<Long, Long> globalAvg;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void initState(KeyValueState<String, Pair<Long, Long>> state) {
            this.state = state;
            globalAvg = state.get(STATE_KEY, Pair.of(0L, 0L));
            LOG.info("initState with global avg [" + (double) globalAvg._1 / globalAvg._2 + "]");
        }

        @Override
        public void execute(TupleWindow window) {
            // iterate over the tuples in window
            Iterator<Tuple> it = window.getIter();
            int sum = 0;
            int count = 0;
            while (it.hasNext()) {
                Tuple tuple = it.next();
                sum += tuple.getInteger(0);
                ++count;
            }
            globalAvg = Pair.of(globalAvg._1 + sum, globalAvg._2 + count);
            state.put(STATE_KEY, globalAvg);
            collector.emit(new Values(new Averages((double) globalAvg._1 / globalAvg._2, (double) sum / count)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Random rand = new Random();
        // generate random numbers
        builder.setSpout("spout", new RandomIntegerSpout());
        // emits sliding window and global averages
        builder.setBolt("avgbolt", new AvgBolt()
                .withWindow(new Duration(5, TimeUnit.SECONDS), new Duration(3, TimeUnit.SECONDS))
                .withPersistence()
                .withMaxEventsInMemory(25000)
            , 1).shuffleGrouping("spout");
        // print the values to stdout
        builder.setBolt("printer", (x, y) -> System.out.println(x.getValue(0)), 1).shuffleGrouping("avgbolt");

        Config conf = new Config();
        conf.setDebug(false);

        // redis for state persistence
        conf.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");

        String topoName = "test";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }
        conf.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }

}
