package com.wolffy.spark.structured.streming.project.mock.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class MockRealtime {

    // 模拟实时数据
    public static List<String> mockRealTimeData() {
        // 数据放到List里
        List<String> listData = new ArrayList<>();

        RandomOptions<CityInfo> randomOpts = new RandomOptions<>();

//        HashMap<CityInfo, Integer> optionsWithWeights = new HashMap<CityInfo, Integer>();

//        randomOpts.options.add(new CityInfo(1, "北京", "华北"));
//        randomOpts.options.add(new CityInfo(2, "上海", "华东"));
//        randomOpts.options.add(new CityInfo(3, "广州", "华南"));
//        randomOpts.options.add(new CityInfo(4, "深圳", "华南"));
//        randomOpts.options.add(new CityInfo(5, "杭州", "华中"));

        // 初始化城市信息及权重
//        optionsWithWeights.put(randomOpts.options.get(0), 30);
//        optionsWithWeights.put(randomOpts.options.get(1), 30);
//        optionsWithWeights.put(randomOpts.options.get(2), 10);
//        optionsWithWeights.put(randomOpts.options.get(3), 20);
//        optionsWithWeights.put(randomOpts.options.get(4), 10);

        for (int i = 1; i <= 50; i++) {

            long timestamp = System.currentTimeMillis();


            CityInfo cityInfo = randomOpts.getRandomOption();

            String area = cityInfo.getArea();
            String city = cityInfo.getCityName();

            int userid = RandomNumUtil.randomInt(100, 105);
            int adid = RandomNumUtil.randomInt(1, 5);

            listData.add(String.format("%d,%s,%s,%d,%d", timestamp, area, city, userid, adid));
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return listData;
    }

    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) {

        List<String> list = mockRealTimeData();
        for (String s : list) {
            System.out.println(s);
        }


//        String topic = "ads_log";
//        KafkaProducer<String, String> producer = createKafkaProducer();
//
//        while (true) {
//            List<String> messages = mockRealTimeData();
//            for (String msg : messages) {
//                producer.send(new ProducerRecord<>(topic, msg));
//                try {
//                    TimeUnit.MILLISECONDS.sleep(100);
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//            try {
//                TimeUnit.SECONDS.sleep(1);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        }
//
//

    }
}

