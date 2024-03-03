package com.wolffy.spark.sparkcore.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test03_broadcost {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 广播变量（分布式只读变量）

        // 3 编写代码
        List<Integer> list = Arrays.asList(5, 6, 7);
        Broadcast<List<Integer>> broadcast = sc.broadcast(list);

        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(2, 6, 4, 9, 10, 3, 7, 8), 4);

        JavaRDD<Integer> filter = javaRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                List<Integer> value = broadcast.value();
                return value.contains(v1);
            }
        });

        filter.collect().forEach(System.out::println);

        // 4 关闭资源
        sc.stop();
    }
}
