package com.wolffy.sparkstreaming.ch1;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Test01_Socket {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
        Duration batchDuration = new Duration(10000);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);

        JavaReceiverInputDStream<String> linesDStream = jssc
                .socketTextStream("localhost", 9999);

        JavaDStream<String> words = linesDStream
                .flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words
                .mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs
                .reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

    }
}
