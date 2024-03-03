package com.wolffy.spark.sparksql.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.udaf;

public class Test02_udaf {
    public static void main(String[] args) throws AnalysisException {
        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]");

        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3 编写代码

        spark.udf().register("myAvg", udaf(new MyAvg(), Encoders.LONG()));

        Dataset<Row> dataset = spark.read().json("./src/main/resources/input/user.json");
        dataset.createTempView("user_info");

        spark.sql("select myAvg(age) from user_info").show();

        // 4 关闭资源
        spark.close();
    }
}
