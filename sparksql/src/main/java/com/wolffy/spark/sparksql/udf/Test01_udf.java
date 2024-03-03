package com.wolffy.spark.sparksql.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.spark.sql.functions.udf;

public class Test01_udf {
    public static void main(String[] args) throws AnalysisException {
        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]");

        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3 编写代码
        UserDefinedFunction now = udf(new UDF0<String>() {
            @Override
            public String call() throws Exception {
                long millis = System.currentTimeMillis();
                // 创建一个Date对象
                Date date = new Date(millis);
                // 创建并初始化SimpleDateFormat对象
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss");
                // 格式化日期
                String dateStr = sdf.format(date);

                return String.valueOf(dateStr);
            }
        }, DataTypes.StringType);

        UserDefinedFunction addName = udf(new UDF2<Long, String, String>() {
            @Override
            public String call(Long age, String name) throws Exception {
                return age + "_" + name;
            }
        }, DataTypes.StringType);

        spark.udf().register("now", now);
        spark.udf().register("myConcat", addName);

        Dataset<Row> dataset = spark.read().json("./src/main/resources/input/user.json");
        dataset.createTempView("user_info");
        spark.sql("select age, name, now(), myConcat(age, name) from user_info").show();
        // 4 关闭资源
        spark.close();
    }
}
