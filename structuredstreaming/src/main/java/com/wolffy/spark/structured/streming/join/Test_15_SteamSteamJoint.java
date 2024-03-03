package com.wolffy.spark.structured.streming.join;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class Test_15_SteamSteamJoint {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("StreamStreamJoint")
                .getOrCreate();

        Encoder<Tuple3<String, String, Timestamp>> tupleEncoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.TIMESTAMP());

        // 第 1 个 stream
        Dataset<Row> nameSexStream = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 10000)
                .load()
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, Tuple3<String, String, Timestamp>>) line -> {
                    String[] arr = line.split(",");
                    return Arrays.asList(new Tuple3<>(arr[0], arr[1], Timestamp.valueOf(arr[2]))).iterator();
                }, tupleEncoder)
                .toDF("name", "sex", "ts1")
                .withWatermark("ts1", "2 minutes");

        // 第 2 个 stream
        Dataset<Row> nameAgeStream = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 20000)
                .load()
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, Tuple3<String, Integer, Timestamp>>) line -> {
                    String[] arr = line.split(",");
                    return Arrays.asList(new Tuple3<>(arr[0], Integer.parseInt(arr[1]), Timestamp.valueOf(arr[2]))).iterator();
                }, Encoders.tuple(Encoders.STRING(), Encoders.INT(), Encoders.TIMESTAMP()))
                .toDF("name", "age", "ts2")
                .withWatermark("ts2", "1 minutes");

        // join 操作
        Dataset<Row> joinResult = nameSexStream.join(nameAgeStream, "name");

        StreamingQuery query = joinResult.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime(0))
                .start();

        query.awaitTermination();
    }
}
