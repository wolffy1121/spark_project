package com.wolffy.spark.sparkcore.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Test05_rePartition {
    public static void main(String[] args) throws InterruptedException {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 2);

        JavaRDD<Integer> repartition = javaRDD.repartition(1);
//        repartition.saveAsTextFile("output1");

//        JavaRDD<Integer> coalesce = javaRDD.coalesce(200, true);
//        coalesce.saveAsTextFile("output1");
        repartition.collect();
        Thread.sleep(99999999);
        // 4 关闭资源
        sc.stop();
    }
}
