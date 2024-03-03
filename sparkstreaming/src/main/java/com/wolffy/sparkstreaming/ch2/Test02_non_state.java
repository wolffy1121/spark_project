package com.wolffy.sparkstreaming.ch2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class Test02_non_state {
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
        JavaDStream<String> flatMap = map.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
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
