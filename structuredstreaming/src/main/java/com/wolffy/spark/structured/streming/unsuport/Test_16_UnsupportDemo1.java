package com.wolffy.spark.structured.streming.unsuport;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;


public class Test_16_UnsupportDemo1 {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SteamSteamJoint")
                .getOrCreate();

        // 第 1 个 stream
        Dataset<Row> nameSexStream = spark.readStream()
                .format("socket")
                .option("host", "hadoop201")
                .option("port", 10000)
                .load()
                .as(Encoders.STRING())
                .toDF("v");

        nameSexStream.createOrReplaceTempView("user");
        spark.sql("SELECT * FROM user LIMIT 1");

        nameSexStream
                .writeStream()
                .outputMode("append")
                .format("console")
                .start()
                .awaitTermination();

    }
}
