package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.Count;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.IndexValueMapper;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.PairFunction;
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder();
//        Stream<Tuple> stream = builder.newStream(new TestWordSpout());
//        stream.map(new Function<Tuple, Values>() {
//            @Override
//            public Values apply(Tuple input) {
//                return new Values(input.getString(0).toUpperCase());
//            }
//        }).forEach(new Consumer<Values>() {
//            @Override
//            public void accept(Values input) {
//                System.out.println(input);
//            }
//        });

//        stream.filter(new Predicate<Tuple>() {
//            @Override
//            public boolean test(Tuple input) {
//                return true;
//            }
//        }).forEach(new Consumer<Tuple>() {
//            @Override
//            public void accept(Tuple input) {
//                System.out.println(input);
//            }
//        });
//
//
//        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));
//
//        stream.filter(new Predicate<String>() {
//            @Override
//            public boolean test(String input) {
//                return input.equals("nathan");
//            }
//        }).map(new Function<String, String>() {
//            @Override
//            public String apply(String input) {
//                return input.toUpperCase();
//            }
//        }).flatMap(new Function<String, Iterable<String>>() {
//            @Override
//            public Iterable<String> apply(String input) {
//                return Arrays.asList(input.split("(?!^)"));
//            }
//        }).forEach(new Consumer<String>() {
//            @Override
//            public void accept(String input) {
//                System.out.println(input);
//            }
//        });
//


        // JAVA 1.8
//        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));
//
//        stream.filter(x -> x.equals("nathan"))
//                .map(x -> x.toUpperCase())
//                .flatMap(x -> Arrays.asList(x.split("(?!^)")))
//                .forEach(x -> System.out.println(x));
//
//        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));
//
//        stream.map(new Function<String, String>() {
//            @Override
//            public String apply(String input) {
//                return input.toUpperCase();
//            }
//        }).aggregate(new Count<String>()).forEach(new Consumer<Long>() {
//            @Override
//            public void accept(Long input) {
//                System.out.println(input);
//            }
//        });


        // JOIN
//        PairStream<String, Long> stream2 = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0))
//                .mapToPair(new PairFunction<String, String, Long>() {
//                    @Override
//                    public Pair<String, Long> apply(String input) {
//                        return new Pair<>(input, 1L);
//                    }
//                }).peek(new Consumer<Pair<String, Long>>() {
//                    @Override
//                    public void accept(Pair<String, Long> input) {
//                        System.out.println("<--" + input);
//                    }
//                });
//
//        Stream<String> stream1 = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));
//
//        PairStream<String, Long> joinStream = stream1.mapToPair(new PairFunction<String, String, Long>() {
//            @Override
//            public Pair<String, Long> apply(String input) {
//                return new Pair<>(input, 1L);
//            }
//        }).window(TumblingWindows.of(Duration.seconds(2))).peek(new Consumer<Pair<String, Long>>() {
//            @Override
//            public void accept(Pair<String, Long> input) {
//                System.out.println("--> " + input);
//            }
//        }).join(stream2).aggregateByKey(new Aggregator<Pair<Long, Long>, Long>() {
//            @Override
//            public Long init() {
//                return 0L;
//            }
//
//            @Override
//            public Long apply(Pair<Long, Long> value, Long aggregate) {
//                return aggregate + 1;
//            }
//        });
//
//        joinStream.forEach(new Consumer<Pair<String, Long>>() {
//            @Override
//            public void accept(Pair<String, Long> input) {
//                System.out.println(new Date() + ": " + input);
//            }
//        });
        // END JOIN


        // WINDOW
//        PairStream<String, Long> stream2 = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0))
//                .mapToPair(new PairFunction<String, String, Long>() {
//            @Override
//            public Pair<String, Long> apply(String input) {
//                return new Pair<>(input, 1L);
//            }
//        }).groupByKey()
//        .window(SlidingWindows.of(BaseWindowedBolt.Count.of(5), BaseWindowedBolt.Count.of(4)))
//                .aggregateByKey(new Count<Long>()).groupByKey();
//
//        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));
//        stream.mapToPair(new PairFunction<String, String, Long>() {
//            @Override
//            public Pair<String, Long> apply(String input) {
//                return new Pair<>(input, 1L);
//            }
//        }).groupByKey()
////                .window(SlidingWindows.of(BaseWindowedBolt.Count.of(5), BaseWindowedBolt.Count.of(3)))
//                .join(stream2)
//                .print();
        //END WINDOW


        builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0))
                .filter(new Predicate<String>() {
                    @Override
                    public boolean test(String input) {
                        return input.length() > 2;
                    }
                })
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Pair<String, String> apply(String input) {
                        return new Pair<>(input, input);
                    }
                }).groupByKey().flatMapValues(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> apply(String input) {
                return Arrays.asList(input.split(""));
            }
        }).print();

//        stream.map(new Function<String, Integer>() {
//            @Override
//            public Integer apply(String input) {
//                return input.length();
//            }
//        }).peek(new Consumer<Integer>() {
//            @Override
//            public void accept(Integer input) {
//                System.out.println("-> " + input);
//            }
//        }).reduce(new Reducer<Integer>() {
//            @Override
//            public Integer apply(Integer val1, Integer val2) {
//                return val1 + val2;
//            }
//        }).forEach(new Consumer<Integer>() {
//            @Override
//            public void accept(Integer input) {
//                System.out.println(input);
//            }
//        });


//        stream.mapToPair(new PairFunction<String, String, String>() {
//            @Override
//            public Pair<String, String> apply(String input) {
//                return new Pair<>(input, input);
//            }
//        }).peek(new Consumer<Pair<String, String>>() {
//            @Override
//            public void accept(Pair<String, String> input) {
//                System.out.println(input);
//            }
//        }).mapValues(new Function<String, String>() {
//            @Override
//            public String apply(String input) {
//                return input.toUpperCase();
//            }
//        }).forEach(new Consumer<Pair<String, String>>() {
//            @Override
//            public void accept(Pair<String, String> input) {
//                System.out.println(input);
//            }
//        });
//
//                .aggregate(new Aggregator<Long, Long>() {
//            @Override
//            public Long init() {
//                return 0L;
//            }
//
//            @Override
//            public Long apply(Long value, Long aggregate) {
//                return value + aggregate;
//            }
//        });
//                .aggregate(new Count<Pair<String, Long>>()).forEach(new Consumer<Pair<Pair<String, Long>, Long>>() {
//            @Override
//            public void accept(Pair<Pair<String, Long>, Long> input) {
//
//            }
//        });

        // PARALLELISM
//        Stream<String> stream = builder.newStream(new TestWordSpout(), 2, new IndexValueMapper<String>(0));
//        stream.mapToPair(new PairFunction<String, String, Long>() {
//            @Override
//            public Pair<String, Long> apply(String input) {
//                return new Pair(input, 1);
//            }
//        }).groupByKey().repartition(3).aggregateByKey(new Count<Long>()).print();
        //

//        // TO
//        String host = "127.0.0.1";
//        int port = 6379;
//
//        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
//                .setHost(host).setPort(port).build();
//
//        RedisStoreMapper storeMapper = new WordCountStoreMapper();
//        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
//
//        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));
//        PairStream<String, Long> s2 = stream.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterable<String> apply(String input) {
//                return Arrays.asList(input.split(" "));
//            }
//        }).mapToPair(new PairFunction<String, String, Long>() {
//            @Override
//            public Pair<String, Long> apply(String input) {
//                return new Pair<>(input, 1L);
//            }
//        })
//                .groupByKey()
//                .window(TumblingWindows.of(Duration.seconds(2)))
//                .aggregateByKey(new Count<Long>());
////                .print();
//               s2.print();
//               s2.aggregate(new Aggregator<Pair<String,Long>, Long>() {
//                   @Override
//                   public Long init() {
//                       return 0L;
//                   }
//
//                   @Override
//                   public Long apply(Pair<String, Long> value, Long aggregate) {
//                       return aggregate + value.getSecond();
//                   }
//               }).print();
////               s2.to(storeBolt);
// END TO
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
//        config.setDebug(true);
        cluster.submitTopology("test", config, builder.build());
        Utils.sleep(100000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

    private static class WordCountStoreMapper implements RedisStoreMapper {
        private RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        public WordCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("key");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            System.out.println(tuple);
            return String.valueOf(tuple.getLongByField("value"));
        }
    }
}
