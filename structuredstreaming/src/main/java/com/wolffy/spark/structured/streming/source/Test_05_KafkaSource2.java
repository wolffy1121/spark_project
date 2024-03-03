package com.wolffy.spark.structured.streming.source;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;


public class Test_05_KafkaSource2 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("KafkaSource")
                .getOrCreate();

        // 得到的 df 的 schema 是固定的: key,value,topic,partition,offset,timestamp,timestampType
        Dataset<Row> rowDataset = spark.readStream()
                .format("kafka") // 设置 kafka 数据源
                .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .option("subscribe", "topic1") // 也可以订阅多个主题:   "topic1,topic2"
                .load()
                .selectExpr("cast(value as string)")
                .as("string");
        rowDataset
                .as(Encoders.STRING())
                .flatMap(
                        (FlatMapFunction<String, String>)
                                s -> Arrays.asList(s.split(" ")).iterator(),Encoders.STRING()
                )
                .groupBy("value")
                .count();


        rowDataset.writeStream()
                .outputMode("update")
                .format("console")
                .trigger(Trigger.Continuous(1000))
                .start()
                .awaitTermination();

    }
}
