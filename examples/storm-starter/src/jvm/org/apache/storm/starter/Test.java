package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Aggregator;
import org.apache.storm.streams.Consumer;
import org.apache.storm.streams.Count;
import org.apache.storm.streams.FlatMapFunction;
import org.apache.storm.streams.Function;
import org.apache.storm.streams.IndexValueMapper;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.PairFlatMapFunction;
import org.apache.storm.streams.PairFunction;
import org.apache.storm.streams.Predicate;
import org.apache.storm.streams.Reducer;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
//        stream.map(new Function<Tuple, String>() {
//            @Override
//            public String apply(Tuple input) {
//                return input.getString(0);
//            }
//        }).filter(new Predicate<String>() {
//            @Override
//            public boolean test(String input) {
//                return input.equals("nathan");
//            }
//        }).map(new Function<String, String>() {
//            @Override
//            public String apply(String input) {
//                return input.toUpperCase();
//            }
//        }).forEach(new Consumer<String>() {
//            @Override
//            public void accept(String input) {
//                System.out.println(input);
//            }
//        });
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


        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));

//        stream.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
//            @Override
//            public Iterable<Pair<String, String>> apply(String input) {
//                List<Pair<String, String>> res = new ArrayList<>();
//                for (String c: input.split("(?!^)")) {
//                    res.add(new Pair<>(c, input));
//                }
//                return res;
//            }
//        }).forEach(new Consumer<Pair<String, String>>() {
//            @Override
//            public void accept(Pair<String, String> input) {
//                System.out.println(input);
//            }
//        });


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


        stream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Pair<String, String> apply(String input) {
                return new Pair<>(input, input);
            }
        }).peek(new Consumer<Pair<String, String>>() {
            @Override
            public void accept(Pair<String, String> input) {
                System.out.println(input);
            }
        }).mapValues(new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input.toUpperCase();
            }
        }).forEach(new Consumer<Pair<String, String>>() {
            @Override
            public void accept(Pair<String, String> input) {
                System.out.println(input);
            }
        });
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
                        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        cluster.submitTopology("test", config, builder.build());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
