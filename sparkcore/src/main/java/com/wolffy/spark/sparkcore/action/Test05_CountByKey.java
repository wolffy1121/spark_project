package com.wolffy.spark.sparkcore.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class Test05_CountByKey {
    public static void main(String[] args) {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 8),
                        new Tuple2<>("b", 8),
                        new Tuple2<>("a", 8),
                        new Tuple2<>("d", 8)
                ));

        Map<String, Long> map = pairRDD.countByKey();

        System.out.println(map);

        // 4. 关闭sc
        sc.stop();
    }
}
