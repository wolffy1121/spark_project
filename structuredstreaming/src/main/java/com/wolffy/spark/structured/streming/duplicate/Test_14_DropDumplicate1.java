package com.wolffy.spark.structured.streming.duplicate;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple3;
import java.sql.Timestamp;

import java.util.concurrent.TimeoutException;

public class Test_14_DropDumplicate1 {
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

        // 1,2019-09-14 11:50:00,dog -->(1,(2019-09-14 11:50:00,dog))
        Dataset<Row> words = lines.map(
                new MapFunction<String, Tuple3<String, Timestamp, String>>() {
                    @Override
                    public Tuple3<String, Timestamp, String> call(String value) throws Exception {
                        String[] split = value.split(",");
                        String uid = split[0];
                        String timestampStr = split[1];
                        String word = split[2];
                        Timestamp timestamp = Timestamp.valueOf(timestampStr);

                        return new Tuple3<>(uid, timestamp, word);
                    }
                }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP(), Encoders.STRING())
        ).toDF("uid", "ts", "word");


        // 去重重复数据 uid 相同就是重复.  可以传递多个列
        Dataset<Row> windowedCounts = words
                .withWatermark("ts", "2 minutes")
                .dropDuplicates("uid");

        windowedCounts.writeStream()
                .outputMode("update")
                .format("console")
                .start()
                .awaitTermination();

    }
}
