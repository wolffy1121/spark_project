package com.wolffy.spark.sparksql.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Test04_parquet {
    public static void main(String[] args) {
        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]");

        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3 编写代码
        Dataset<Row> dataset = spark.read().json("input/user.json");

//        dataset.write().mode(SaveMode.Overwrite).parquet("output");
        spark.read().parquet("output").show();
        // 4 关闭资源
        spark.close();
    }
}
