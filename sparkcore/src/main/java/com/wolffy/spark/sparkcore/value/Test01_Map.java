package com.wolffy.spark.sparkcore.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class Test01_Map {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> lineRDD = sc.textFile("./src/main/resources/input/1.txt");

        // 两种写法  lambda表达式写法(匿名函数)
        JavaRDD<String> mapRDD = lineRDD.map(s -> s + "||");


        // 匿名函数写法
        JavaRDD<String> mapRDD1 = lineRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + "||";
            }
        });
        mapRDD1.collect().forEach(a -> System.out.println(a));

        for (String s : mapRDD.collect()) {
            System.out.println(s);
        }

        // 输出数据的函数写法
        mapRDD1.collect().forEach(a -> System.out.println(a));
        mapRDD1.collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();
    }
}
