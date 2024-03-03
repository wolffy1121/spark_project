package com.wolffy.spark.sparksql.bean;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test02_SQL {
    public static void main(String[] args) {

        //1. 创建配置对象
        SparkConf conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]");

        //2. 获取sparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //3. 编写代码
        Dataset<Row> lineDS = spark.read().json("./src/main/resources/input/user.json");

        // 创建视图 => 转换为表格 填写表名
        // 临时视图的生命周期和当前的sparkSession绑定
        // orReplace表示覆盖之前相同名称的视图
        lineDS.createOrReplaceTempView("t1");

        // 支持所有的hive sql语法,并且会使用spark的又花钱
        Dataset<Row> result = spark.sql("select * from t1 where age > 18");

        result.show();

        //4. 关闭sparkSession
        spark.close();
    }
}
