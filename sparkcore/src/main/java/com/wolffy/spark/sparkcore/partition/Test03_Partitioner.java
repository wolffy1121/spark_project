package com.wolffy.spark.sparkcore.partition;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test03_Partitioner {

    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码
        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("A", 1));
        list.add(new Tuple2<>("B", 1));
        list.add(new Tuple2<>("C", 1));
        list.add(new Tuple2<>("D", 1));
        //1 直接创建pairRDD  用的不是分区器， 就叫分区规则
        // 分区器谁有？ 转化算子
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(list, 2);


        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> javaRDD = sc.parallelize(list1, 3);

        // 获取PairRDD的分区器


        // 分区器： keyvalue类型算子才有， value类型没有
        Optional<Partitioner> partitioner = pairRDD.partitioner();
        System.out.println(partitioner);
        System.out.println("=====groupByKey 分区器=====");
        JavaPairRDD<String, Iterable<Integer>> groupByKey = pairRDD.groupByKey();
        System.out.println(groupByKey.partitioner());

        System.out.println("=====map 分区器=====");
        JavaRDD<Integer> map = javaRDD.map(x -> x * 2);
        System.out.println(map.partitioner());

        JavaPairRDD<Integer, Iterable<Integer>> groupBy = javaRDD.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        });

        System.out.println("=====groupBy 分区器=====");
        System.out.println(groupBy.partitioner());


        JavaRDD<Integer> sortBy = javaRDD.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, false, 1);

        System.out.println("=====sortBy 分区器=====");
        System.out.println(sortBy.partitioner());

        JavaPairRDD<String, Integer> sortByKey = pairRDD.sortByKey();
        System.out.println("=====sortByKey 分区器=====");
        System.out.println(sortByKey.partitioner());

        JavaPairRDD<String, Integer> mapValues = pairRDD.mapValues(x -> x);
        System.out.println("=====mapValues 分区器=====");
        System.out.println(mapValues.partitioner());
        // 4 关闭资源
        sc.stop();
    }
}
