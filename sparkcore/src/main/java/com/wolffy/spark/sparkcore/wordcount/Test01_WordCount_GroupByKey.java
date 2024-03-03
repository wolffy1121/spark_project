package com.wolffy.spark.sparkcore.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Test01_WordCount_GroupByKey {
    public static void main(String[] args) throws InterruptedException {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码
        JavaRDD<String> lineRDD = sc.textFile("./sparkcore/src/main/resources/input/1.txt", 2);
        // hello word spark flink hadoop zookeeper kafka
        JavaRDD<String> flatMap = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return Arrays.stream(split).iterator();
            }
        });

        JavaRDD<String> filter = flatMap.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String word) throws Exception {
                return !"".equals(word) && word != null;
            }
        });
        // (word,1)
        JavaPairRDD<String, Integer> mapToPair = filter.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupByKey = mapToPair.groupByKey();

        JavaPairRDD<String, Long> mapValues = groupByKey.mapValues(new Function<Iterable<Integer>, Long>() {
            @Override
            public Long call(Iterable<Integer> v1) throws Exception {
                Long sum = 0L;
                for (Integer elem : v1) {
                    sum += elem;
                }
                return sum;
            }
        });


        mapValues.take(20).forEach(System.out::println);
        Thread.sleep(999999999);

        // 4 关闭资源
        sc.stop();
    }
}
