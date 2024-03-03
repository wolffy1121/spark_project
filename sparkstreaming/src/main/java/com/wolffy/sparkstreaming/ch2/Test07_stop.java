package com.wolffy.sparkstreaming.ch2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class Test07_stop {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Duration.apply(3000));

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


        map.print();

        // 监控线程， 监控一个文件夹， stopSpark， 当这个文件夹存在的时候， 那么我们就关闭程序
        // 不存在的时候就一直运行
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    File file = new File("stopSpark");

                    if (file.exists()) {
                        jssc.stop(true, true);
                        System.exit(0);
                    }
                }
            }
        }).start();

        // 开启Streaming运行环境
        jssc.start();
        jssc.awaitTermination();

    }
}
