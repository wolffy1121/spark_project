package com.wolffy.spark.sparkcore.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;


public class Test05_Distinct {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(Arrays.asList(1, 2, 33,3,3,3,3,3, 4, 5, 6), 1);

        // 底层使用分布式分组去重  所有速度比较慢,但是不会OOM
        JavaRDD<Integer> distinct = integerJavaRDD.distinct();

        distinct. collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
