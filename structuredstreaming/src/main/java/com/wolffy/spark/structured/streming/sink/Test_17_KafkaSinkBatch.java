package com.wolffy.spark.structured.streming.sink;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class Test_17_KafkaSinkBatch {

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
//        spark.sparkContext().parallelize(Arrays.asList("hello hello atguigu", "atguigu hello"));


        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split("\\W+");
                ArrayList<String> result = new ArrayList<>();
                for (String s1 : split) {
                    result.add(s1);
                }
                return result.iterator();
            }
        }, Encoders.STRING());

        words.writeStream()
                .outputMode("append")
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("checkpointLocation", "src/main/resources/filesink/checkpoint")
                .option("topic", "first")
                .start()
                .awaitTermination();

    }
}
