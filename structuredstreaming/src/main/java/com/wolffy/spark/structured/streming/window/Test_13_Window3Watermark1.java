package com.wolffy.spark.structured.streming.window;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Test_13_Window3Watermark1 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("watermark")
                .getOrCreate();
        Dataset<String> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        // 输入的数据中包含时间戳, 而不是自动添加的时间戳
        // 2024-01-25 10:20:00,hello hello hello
        Dataset<Row> words = lines
                .flatMap(
                        (FlatMapFunction<String, Tuple2<String, Timestamp>>) line -> {
                            String[] split = line.split(",");
                            String timestampStr = split[0];
                            String wordList = split[1];

                            // 将timestamp字符串转换为Timestamp对象
                            Timestamp timestamp = Timestamp.valueOf(timestampStr);

                            // 使用Java 8 Stream API处理wordList
                            return Arrays.stream(wordList.split(" "))
                                    .map(word -> new Tuple2<>(word, timestamp))
                                    .iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("word", "timestamp");

        Dataset<Row> windowedCounts = words
                .withWatermark("timestamp", "10 minutes")
                .groupBy(
                        functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
                        words.col("word")
                )

                .count();
        //        Dataset<Tuple2<String, String>> tuple2Dataset = lines.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
//            @Override
//            public Iterator<Tuple2<String, String>> call(String s) throws Exception {
//
//                List<Tuple2<String, String>> result = new ArrayList<>();
//
//                String[] splits = s.split(",");
//                String timeStampStr = splits[0];
//                String value = splits[1];
//
//                for (String s1 : value.split(" ")) {
//                    result.add(new Tuple2<>(timeStampStr, s1));
//                }
//                return result.iterator();
//            }
//        }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        windowedCounts.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .trigger(Trigger.ProcessingTime(2000))
                .start()
                .awaitTermination();

    }

}
