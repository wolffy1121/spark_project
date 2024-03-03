package com.wolffy.spark.sparkcore.cache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class Test03_checkpoint {
    public static void main(String[] args) throws InterruptedException {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setCheckpointDir("/ck");

        // 3 编写代码

        JavaRDD<String> lineRDD = sc.textFile("./sparkcore/src/main/resources/input/user.txt", 2);

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

        flatMapToPair.cache();
        flatMapToPair.checkpoint();
        flatMapToPair.take(1).forEach(System.out::println);
        System.out.println(flatMapToPair.toDebugString());
        flatMapToPair.unpersist();
        flatMapToPair.take(1).forEach(System.out::println);




        Thread.sleep(999999999);

        // 4 关闭资源
        sc.stop();
    }
}
