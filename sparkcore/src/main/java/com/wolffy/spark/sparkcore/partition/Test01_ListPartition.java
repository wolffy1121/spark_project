package com.wolffy.spark.sparkcore.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Test01_ListPartition {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("hehhe", "py"), 2);
        stringRDD.saveAsTextFile("./src/main/resources/output");
        // 4. 关闭sc
        sc.stop();
    }
}
