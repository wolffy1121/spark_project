package com.wolffy.spark.sparkcore.createrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Test02_File {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> lineRDD = sc.textFile("./sparkcore/src/main/resources/input");
        List<String> result = lineRDD.collect();
        result.forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();
    }
}
