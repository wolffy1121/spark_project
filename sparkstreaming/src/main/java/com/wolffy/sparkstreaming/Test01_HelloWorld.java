package com.wolffy.sparkstreaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.internal.config.Streaming;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.ArrayList;
import java.util.HashMap;


public class Test01_HelloWorld {
    public static void main(String[] args) throws InterruptedException {
        // 创建流环境
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "HelloWorld", Duration.apply(3000));


//        new Streaming
        // 创建配置参数
        HashMap<String, Object> kfakaParams = new HashMap<>();
        kfakaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        kfakaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kfakaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kfakaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "wolffy");
        kfakaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 需要消费的主题
        ArrayList<String> topics= new ArrayList<>();
        topics.add("topicA");

        JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kfakaParams));

        directStream
                .map(new Function<ConsumerRecord<String, String>, String>() {
                    @Override
                    public String call(ConsumerRecord<String, String> v1) throws Exception {
                        return v1.value();
                    }
                })
                .print(100);

        // 执行流的任务
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
