package com.wolffy.spark.sparkcore.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class Test01_acc_foreach {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        LongAccumulator acc = JavaSparkContext.toSparkContext(sc).longAccumulator();
        LongAccumulator acc2 = JavaSparkContext.toSparkContext(sc).longAccumulator();

        // 3 编写代码
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        JavaRDD<Integer> map = javaRDD.map(x -> x);

        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                acc.add(integer);
            }
        });

        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                acc2.add(integer);
            }
        });

        System.out.println(acc.value());
        System.out.println(acc2.value());

        // 4 关闭资源
        sc.stop();
    }
}
