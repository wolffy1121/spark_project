package com.wolffy.spark.structured.streming.wc;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class Test_02_WordCount {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        // Generate running word count
//        Dataset<Row> wordCounts = words.groupBy("value").count();
        lines.createOrReplaceTempView("w");
        Dataset<Row> wordCounts = spark.sql("select * from w");

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("append")
                .format("console")
                .start();
        /*
            compete
                全部输出, 必须有聚合
            append
                追加模式.  只输出那些将来永远不可能再更新的数据
                没有聚合的时候, append和update一致
                有聚合的时候, 一定要有水印才能使用append
            update
                只输出变化的部分
        */

        query.awaitTermination();
    }
}
