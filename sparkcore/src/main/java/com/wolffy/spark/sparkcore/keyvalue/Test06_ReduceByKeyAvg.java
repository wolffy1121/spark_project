package com.wolffy.spark.sparkcore.keyvalue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class Test06_ReduceByKeyAvg {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("hi", 96),
                        new Tuple2<>("hi", 97),
                        new Tuple2<>("hello", 95),
                        new Tuple2<>("hello", 195)
                ));

        // ("hi",96)-(hi,(193,1))
        // v1=96---(96,1)
        // 该函数将JavaPairRDD中的每个值映射为一个Tuple2对象，其中第一个元素为原始值，第二个元素为1。返回一个新的JavaPairRDD，其中每个值都是一个Tuple2对象。
        JavaPairRDD<String, Tuple2<Integer, Integer>> tuple2JavaPairRDD = javaPairRDD.mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
                return new Tuple2<>(v1, 1);
            }
        });
        // 聚合RDD
        // (hi,(96,1)),(hi,(97,1))--(hi,(193,2))
        JavaPairRDD<String, Tuple2<Integer, Integer>> reduceRDD = tuple2JavaPairRDD.reduceByKey(
                new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
                    }
                });

        // 相除
        JavaPairRDD<String, Double> result = reduceRDD.mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Integer, Integer> v1) throws Exception {
                return (new Double(v1._1) / v1._2);
            }
        });

        result.collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
