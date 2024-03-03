package com.wolffy.spark.sparkcore.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

public class Test03_WordCount_ReduceByKey_Lambda {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码
        sc
                .textFile("./sparkcore/src/main/resources/input/1.txt")
                .flatMapToPair(line -> {
                    String[] split = line.split(" ");
                    ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
                    for (String word : split) {
                        result.add(new Tuple2<>(word, 1));
                    }
                    return result.iterator();
                })
                .filter(v1 -> !"".equals(v1._1) && v1._1 != null)
                .reduceByKey((sum, elem) -> sum + elem)
                .collect()
                .forEach(System.out::println);
        // 4 关闭资源
        sc.stop();
    }
}
