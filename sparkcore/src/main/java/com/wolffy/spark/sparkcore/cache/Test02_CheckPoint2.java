package com.wolffy.spark.sparkcore.cache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Test02_CheckPoint2 {
    public static void main(String[] args) {

        // 修改用户名称
        System.setProperty("HADOOP_USER_NAME","wolffy");

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 需要设置路径.需要提前在HDFS集群上创建/checkpoint路径
        sc.setCheckpointDir("hdfs://hadoop102:8020/checkpoint");


        // 3. 编写代码
        JavaRDD<String> lineRDD = sc.textFile("input/2.txt");

        JavaPairRDD<String, Long> tupleRDD = lineRDD.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                return new Tuple2<String, Long>(s, System.currentTimeMillis());
            }
        });

        // 查看血缘关系
        System.out.println(tupleRDD.toDebugString());

        // 增加检查点避免计算两次
        tupleRDD.cache();

        // 进行检查点
        tupleRDD.checkpoint();

        tupleRDD. collect().forEach(System.out::println);

        System.out.println(tupleRDD.toDebugString());
        // 第二次计算
        tupleRDD. collect().forEach(System.out::println);
        // 第三次计算
        tupleRDD. collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
