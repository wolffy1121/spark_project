package com.wolffy.spark.sparksql.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Test01_SparkSeesion {
    public static void main(String[] args) {

        // 方式一
        SparkConf conf = new SparkConf().setAppName("spark-sql").setMaster("local[2]");
        SparkSession spark1 = SparkSession.builder().config(conf).getOrCreate();

        // 方式二
        SparkSession spark2 = SparkSession.builder().appName("spark-sql").master("local[2]").getOrCreate();
        System.out.println(spark1);
        System.out.println(spark2);


        spark1.close();
        spark2.close();
    }
}
