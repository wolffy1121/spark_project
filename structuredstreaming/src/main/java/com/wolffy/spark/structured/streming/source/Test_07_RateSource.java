package com.wolffy.spark.structured.streming.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class Test_07_RateSource {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RateSource")
                .getOrCreate();


        Dataset<Row> rowDataset = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 1000)
                .option("rampUpTime", 1)
                .option("numPartitions", 3)
                .load();

        rowDataset.writeStream()
                .format("console")
                .outputMode("update")
                .option("truncate", false)
                .start()
                .awaitTermination();



    }
}
