package com.wolffy.spark.sparkcore.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Test01_list {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码


        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 3);


        javaRDD.saveAsTextFile("output");
        // 4 关闭资源
        sc.stop();
    }
}
