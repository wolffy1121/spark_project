package com.wolffy.sparkstreaming.ch2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class Test05_window {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Duration.apply(3000));

        jssc.checkpoint("ck");

        // PreferBrokers(),   在kafka集群topic的leader所在的机器创建exector执行spark的task
        // PreferConsistent() 找到空闲机器， 启动exector
        // PreferFixed()      手动指定 exector创建的位置
        //写代码

        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");

        // 封装kafka参数
        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "sparkStreaming");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> map = directStream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                return consumerRecord.value();
            }
        });

        JavaPairDStream<String, Integer> mapToPair = map.mapToPair(v1 -> new Tuple2<>(v1, 1));

        JavaPairDStream<String, Integer> window = mapToPair.window(Duration.apply(6000), Duration.apply(3000));
        JavaPairDStream<String, Integer> reduceByKey = window.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer sum, Integer elem) throws Exception {
                return sum + elem;
            }
        });


        reduceByKey.print();
        // 开启Streaming运行环境
        jssc.start();
        jssc.awaitTermination();
    }
}
