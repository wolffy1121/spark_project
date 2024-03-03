package com.wolffy.spark.sparkcore.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Test01_Collect {
    public static void main(String[] args) {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(
                Arrays.asList(1, 2, 3, 4), 2);

        List<Integer> collect = integerJavaRDD.collect();
        collect.forEach(System.out::println);

        for (Integer integer : collect) {
            System.out.println(integer);
        }

        // 4. 关闭sc
        sc.stop();
    }
}