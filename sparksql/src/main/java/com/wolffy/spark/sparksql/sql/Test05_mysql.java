package com.wolffy.spark.sparksql.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class Test05_mysql {
    public static void main(String[] args) {
        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]");

        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3 编写代码

        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "123456");

        Dataset<Row> dataset = spark
                .read()
                .jdbc("jdbc:mysql://hadoop102:3306/gmall?useSSL=false", "base_province", properties);

        dataset.show();


        dataset
                .write()
                .mode(SaveMode.Append)
                .jdbc("jdbc:mysql://hadoop102:3306/gmall?useSSL=false", "test1_province", properties);

        // 4 关闭资源
        spark.close();
    }
}
