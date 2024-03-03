package com.wolffy.spark.structured.streming.sink;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class Test_16_FileSink {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SteamSteamJoint")
                .getOrCreate();

        Dataset<String> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        Dataset<Tuple2<String, String>> tuple2Dataset = lines.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                String[] split = s.split("\\W+");
                ArrayList<Tuple2<String, String>> result = new ArrayList<>();
                for (String value : split) {
                    result.add(new Tuple2<>(value, value.toUpperCase()));
                }
                return result.iterator();
            }
        }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
        tuple2Dataset.toDF("原单词", "大写单词");


        tuple2Dataset.writeStream()
                .outputMode("append")
                .format("json") //  // 支持 "orc", "json", "csv"
                .option("path", "src/main/resources/filesink") // 输出目录
                .option("checkpointLocation", "src/main/resources/filesink/ck1")  // 必须指定 checkpoint 目录
                .start()
                .awaitTermination();
    }
}
