package com.wolffy.spark.sparksql.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Test06_hive {
    public static void main(String[] args) {

        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSQL").setMaster("local[2]");
        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

        // 3 编写代码
        spark.sql("show databases").show();
//        spark.sql("create table user_info(name string, age bigint)");
        spark.sql("show tables").show();
//        spark.sql("insert into table user_info values('zs', 18)");
//        spark.sql("select * from user_info").show();

        // 4 关闭资源
        spark.close();
    }
}
