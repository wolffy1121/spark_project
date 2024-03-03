package com.wolffy.spark.sparkcore.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class Test01_acc {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        LongAccumulator acc = JavaSparkContext.toSparkContext(sc).longAccumulator();
        LongAccumulator acc2 = JavaSparkContext.toSparkContext(sc).longAccumulator();


        //driver
        final int[] sum = {0};

        // 3 编写代码
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                acc.add(v1);
//                acc2.add(v1);
                System.out.println(acc.value());
                return v1;
            }
        });
        map.collect();
//        acc.reset();
//        map.collect().forEach(System.out::println);

        System.out.println("--------------------------");
        System.out.println(acc.value());
//        System.out.println(acc2.value());


//        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
//            int sum = 0;
//            @Override
//            public Integer call(Integer v1) throws Exception {
//                sum += v1;
//                System.out.println(sum);
//                return v1;
//            }
//        });
//        map.collect();
        //System.out.println(sum[0]);
        // 4 关闭资源
        sc.stop();
    }
}
