package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Consumer;
import org.apache.storm.streams.Function;
import org.apache.storm.streams.IndexValueMapper;
import org.apache.storm.streams.Predicate;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.testing.TestWordSpout;
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
        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));

        stream.filter(new Predicate<String>() {
            @Override
            public boolean test(String input) {
                return input.equals("nathan");
            }
        }).map(new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input.toUpperCase();
            }
        }).flatMap(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String input) {
                return Arrays.asList(input.split("(?!^)"));
            }
        }).forEach(new Consumer<String>() {
            @Override
            public void accept(String input) {
                System.out.println(input);
            }
        });



        // JAVA 1.8
//        Stream<String> stream = builder.newStream(new TestWordSpout(), new IndexValueMapper<String>(0));
//
//        stream.filter(x -> x.equals("nathan"))
//                .map(x -> x.toUpperCase())
//                .flatMap(x -> Arrays.asList(x.split("(?!^)")))
//                .forEach(x -> System.out.println(x));

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        cluster.submitTopology("test", config, builder.build());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
