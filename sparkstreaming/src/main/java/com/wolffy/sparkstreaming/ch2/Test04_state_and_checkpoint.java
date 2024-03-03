package com.wolffy.sparkstreaming.ch2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
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

public class Test04_state_and_checkpoint {
    public static void main(String[] args) throws InterruptedException {

        JavaStreamingContext jssc = JavaStreamingContext
                .getOrCreate("ck", new Function0<JavaStreamingContext>() {
                    @Override
                    public JavaStreamingContext call() throws Exception {
                        SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]");
                        JavaSparkContext sc = new JavaSparkContext(sparkConf);
                        JavaStreamingContext jssc = new JavaStreamingContext(sc, Duration.apply(3000));
                        jssc.checkpoint("ck");

                        ////////////// 在恢复完环境和return 之前写业务逻辑代码  //////////////////

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

                        JavaPairDStream<String, Integer> mapToPair = map.mapToPair(new PairFunction<String, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(String s) throws Exception {
                                return new Tuple2<>(s, 1);
                            }
                        });
                        JavaPairDStream<String, Integer> updateStateByKey = mapToPair.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                            @Override
                            public Optional<Integer> call(List<Integer> elems, Optional<Integer> state) throws Exception {
                                Integer newState = state.orElse(0);

                                for (Integer elem : elems) {
                                    newState += elem;
                                }

                                return Optional.of(newState);
                            }
                        });
                        updateStateByKey.print();

                        ///////////////////////////////////////////////////////////////////
                        return jssc;
                    }
                });


        jssc.start();
        jssc.awaitTermination();
    }
}
