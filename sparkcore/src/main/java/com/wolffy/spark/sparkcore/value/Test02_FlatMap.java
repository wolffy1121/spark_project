package com.wolffy.spark.sparkcore.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class Test02_FlatMap {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        ArrayList<List<String>> arrayLists = new ArrayList<>();

        // 两个列表
        arrayLists.add(Arrays.asList("1","2","3"));
        arrayLists.add(Arrays.asList("4","5","6"));

        JavaRDD<List<String>> listJavaRDD = sc.parallelize(arrayLists,2);

        // 对于集合嵌套的RDD 可以将元素打散
        // 泛型为打散之后的元素类型
        JavaRDD<String> stringJavaRDD = listJavaRDD.flatMap(new FlatMapFunction<List<String>, String>() {
            @Override
            public Iterator<String> call(List<String> strings) throws Exception {
                return strings.iterator();
            }
        });

        stringJavaRDD. collect().forEach(System.out::println);

        // 通常情况下需要自己将元素转换为集合
        JavaRDD<String> lineRDD = sc.textFile("./src/main/resources/input/2.txt");

        JavaRDD<String> stringJavaRDD1 = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Arrays.asList(s1).iterator();
            }
        });

        stringJavaRDD1. collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();
    }
}
