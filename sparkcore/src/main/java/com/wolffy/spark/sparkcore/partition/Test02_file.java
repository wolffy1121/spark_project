package com.wolffy.spark.sparkcore.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Test02_file {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码

        // long totalSize = 7 byte;  文件总长度（换行符）
        // long goalSize =  7 / 2 = 3         totalSize / (numSplits == 0 ? 1 : numSplits);
        // long minSize = 1
        //long blockSize = 128M

        // long splitSize = 2 byte    Math.max(minSize, Math.min(goalSize, blockSize));



        JavaRDD<String> javaRDD = sc.textFile("input/1.txt", 2);

        javaRDD.saveAsTextFile("output");

        // 4 关闭资源
        sc.stop();
    }
}
