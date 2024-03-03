package com.wolffy.spark.sparkcore.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class Test06_SortBy {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(Arrays.asList(5, 8, 1, 11, 20), 2);

        // (1)泛型为以谁作为标准排序  (2) true为正序  (3) 排序之后的分区个数
        JavaRDD<Integer> sortByRDD = integerJavaRDD.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, true, 2);

        sortByRDD.collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
