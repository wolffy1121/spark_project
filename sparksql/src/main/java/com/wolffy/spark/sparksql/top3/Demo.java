package com.wolffy.spark.sparksql.top3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class Demo {
    public static void main(String[] args) throws AnalysisException {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql")
                .setMaster("local[1]");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> dataset = spark.read().text("src/main/resources/input/user_visit_action.txt");


        dataset
                .write()
                .mode(SaveMode.Overwrite)
                .json("src/main/resources/output1");
    }
}
