package com.wolffy.spark.sparkcore.partition;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class Test04_CustomPartitioner {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码

        ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, 1));list.add(new Tuple2<>(1, 1));list.add(new Tuple2<>(1, 1));
        list.add(new Tuple2<>(1, 1));list.add(new Tuple2<>(1, 1));list.add(new Tuple2<>(1, 1));
        list.add(new Tuple2<>(1, 1));list.add(new Tuple2<>(1, 1));list.add(new Tuple2<>(1, 1));
        list.add(new Tuple2<>(3, 5));
        list.add(new Tuple2<>(5, 5));
        list.add(new Tuple2<>(4, 5));
        list.add(new Tuple2<>(2, 2));list.add(new Tuple2<>(2, 2));list.add(new Tuple2<>(2, 2));
        list.add(new Tuple2<>(2, 2));list.add(new Tuple2<>(2, 2));list.add(new Tuple2<>(2, 2));
        list.add(new Tuple2<>(2, 2));list.add(new Tuple2<>(2, 2));list.add(new Tuple2<>(2, 2));

        JavaPairRDD<Integer, Integer> pairRDD = sc.parallelizePairs(list, 2);

        // RangePartitioner
        JavaPairRDD<Integer, Integer> sortByKey = pairRDD.sortByKey();
//        JavaPairRDD<Integer, Integer> partitionBy = sortByKey.partitionBy(new HashPartitioner(2));
        JavaPairRDD<Integer, Integer> partitionBy = sortByKey.partitionBy(new CustomPartitioner(2));

        JavaRDD<String> withIndex = partitionBy.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer v1, Iterator<Tuple2<Integer, Integer>> v2) throws Exception {
                ArrayList<String> result = new ArrayList<>();
                while (v2.hasNext()) {
                    Tuple2<Integer, Integer> next = v2.next();
                    result.add(v1 + "----" + next);
                }
                return result.iterator();
            }
        }, false);
        withIndex.collect().forEach(System.out::println);

        // 4 关闭资源
        sc.stop();
    }
}
