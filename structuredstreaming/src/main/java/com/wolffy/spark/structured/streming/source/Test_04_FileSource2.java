package com.wolffy.spark.structured.streming.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class Test_04_FileSource2 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("FileSource")
                .getOrCreate();
        // 定义 Schema, 用于指定列名以及列中的数据类型
        StructType userSchema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.LongType)
                .add("job", DataTypes.StringType);

        Dataset<Row> user = spark.readStream()
                .format("csv")
                .schema(userSchema)
                .load("src/main/resources/input");


        user.writeStream()
                .outputMode("update")
                .trigger(Trigger.ProcessingTime(1000)) // 触发器 数字表示毫秒值. 0 表示立即处理
                .format("console")
                .start()
                .awaitTermination();


    }
}
