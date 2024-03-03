package com.wolffy.spark.sparkcore.cache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class Test01_cache {
    public static void main(String[] args) throws InterruptedException {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码

        JavaRDD<String> lineRDD = sc.textFile("input/1.txt", 2);

        JavaPairRDD<String, Integer> flatMapToPair = lineRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {
                String[] split = line.split(" ");
                ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
                for (String word : split) {
                    result.add(new Tuple2<>(word, 1));
                }

                return result.iterator();
            }
        });

        //  // 使用flatMapToPair方法将元素映射为Pair，并且将结果进行缓存
        flatMapToPair.cache();
        flatMapToPair.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Integer> filter = flatMapToPair.filter(v1 -> true);
        filter.collect();
        System.out.println(filter.toDebugString());

        flatMapToPair.map(v1 -> v1).collect();


        Thread.sleep(999999999);

        // 4 关闭资源
        sc.stop();
    }
}
