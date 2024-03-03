package com.wolffy.spark.sparkcore.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class Test02_WordCount_ReduceByKey {

    public static void main(String[] args) throws InterruptedException {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码

        JavaRDD<String> lineRDD = sc.textFile("./sparkcore/src/main/resources/input/1.txt", 2);

        // 打散并转换成二元组()-->(word,1)
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

        JavaPairRDD<String, Integer> filter = flatMapToPair.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                return !"".equals(v1._1) && v1._1 != null;
            }
        });

        JavaPairRDD<String, Integer> reduceByKey = filter.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer sum, Integer elem) throws Exception {
                return sum + elem;
            }
        });

        reduceByKey.take(100).forEach(System.out::println);

        Thread.sleep(999999999);

        // 4 关闭资源
        sc.stop();
    }
}
