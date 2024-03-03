package com.wolffy.spark.sparksql.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test01_sql {
    public static void main(String[] args) throws AnalysisException {
        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]");

        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        SparkSession spark3 = spark.newSession();

        System.out.println(spark);
        System.out.println(spark3);

        // 3 编写代码
        Dataset<Row> dataset = spark.read().json("./sparksql/src/main/resources/input/user.json");


        // 普通临时视图， 和全局临时视图的生命周期不同
        // 普通临时视图：他的生命周期跟SparkSession有关
        // 全局临时视图: 他的生命周期跟application有关

        dataset.createTempView("user_info");
//        dataset.createOrReplaceTempView("user_info");

        dataset.createGlobalTempView("user_info");
//        dataset.createOrReplaceGlobalTempView("user_info");

        // select * from user_info
        Dataset<Row> sql = spark.sql("select age + 1, name from user_info");
        sql.show();
        Dataset<Row> sql1 = spark3.sql("select count(*) from global_temp.user_info");
        sql1.show();

        // 4 关闭资源
        spark.close();
    }
}
