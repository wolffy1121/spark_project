package com.wolffy.spark.structured.streming.window;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.util.concurrent.TimeoutException;

public class Test_12_Window3 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // 创建一个SparkSession对象，用于与Apache Spark进行交互和分析
        SparkSession spark = SparkSession
                .builder()
                .appName("window-demo")
                .master("local[*]")
                .getOrCreate();
        // 以socket方式从指定主机和端口读取数据，并将数据转换为DataFrame，同时给数据添加时间戳。
        Dataset<String> inputLines = spark.readStream()
                .format("socket") // 设置数据源
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());
        // 2024-01-28 00:00:00,hello
        // 一个MapFunction对象和对应的输出类型Encoder
        Dataset<Row> words = inputLines
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] splits = s.split(",");
                        if (splits.length != 2) {
                            return null;
                        } else {
                            String timestampStr = splits[0];
                            String value = splits[1];

                            // 这里假设timestamp是一个可解析为日期时间的字符串
                            // 将timestampStr转换为Timestamp类型，这里假设是通过toInstant和Timestamp.from方法
                            // java.time.Instant instant = Instant.parse(timestampStr);
                            // Timestamp timestamp = Timestamp.from(instant);
                            // 实际上Spark会自动处理Timestamp类型，上述转换可能不需要，取决于您的具体需求
                            return new Tuple2<>(timestampStr, value);
                        }
                    }
                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .toDF("timestamp", "word");

        words
                .groupBy(
                        functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
                        words.col("word"))
                .count()
                .orderBy("window")
                .writeStream()
                .format("console")
                .outputMode("complete")
                .option("truncate", false)
                .start()
                .awaitTermination();

    }
}