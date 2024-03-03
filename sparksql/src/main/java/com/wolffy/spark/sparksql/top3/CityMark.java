package com.wolffy.spark.sparksql.top3;

import lombok.Data;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;

public class CityMark extends Aggregator<String, CityMark.Buffer, String> {


    /*
    * 初始化
    * */
    @Override
    public Buffer zero() {
        return new Buffer(0L, new HashMap<>());
    }


    /*
     * 分区内聚合
     * */
    @Override
    public Buffer reduce(Buffer b, String cityName) {
        // 累加地区点击数量
        b.setAreaCount(b.getAreaCount() + 1L);

        // 累加城市点击数量
        Map<String, Long> citysCount = b.getCitysCount();
        citysCount.put(cityName, citysCount.getOrDefault(cityName, 0L) + 1L);

        // 没有必要执行
        b.setCitysCount(citysCount);
        return b;
    }


    /*
     * 分区间聚合
     * */
    @Override
    public Buffer merge(Buffer b1, Buffer b2) {
        b1.setAreaCount(b1.getAreaCount() + b2.getAreaCount());

        // 获取b1 和b2当中的CitysCount
        Map<String, Long> b1CitysCount = b1.getCitysCount();
        Map<String, Long> b2CitysCount = b2.getCitysCount();
        // 将b2的CitysCount 累加到b1的CitysCount
        b2CitysCount.forEach(new BiConsumer<String, Long>() {
            @Override
            public void accept(String cityName, Long cityCount) {
                b1CitysCount.put(cityName, b1CitysCount.getOrDefault(cityName, 0L) +  cityCount);
            }
        });

        // 没有必要执行
        b1.setCitysCount(b1CitysCount);
        return b1;
    }


    /*
     * 计算最终结果
     * 北京21.2%，天津13.2%，其他65.6%
     * */
    @Override
    public String finish(Buffer reduction) {

        // 北京，100    天津，80    河北， 80
        Map<String, Long> citysCount = reduction.getCitysCount();
        Long areaCount = reduction.getAreaCount();

        // TreeMap 只能对Key排序, 并且是顺序排序
        // 将Map集合的数据放到TreeMap当中， 并且Key Value倒转

        TreeMap<Long, String> treeMap = new TreeMap<>();

        // 100，北京    80，天津     80，河北 
        citysCount.forEach(new BiConsumer<String, Long>() {
            @Override
            public void accept(String cityName, Long cityCount) {
                if (!treeMap.containsKey(cityCount)) {
                    treeMap.put(cityCount, cityName);
                } else {
                    treeMap.put(cityCount, treeMap.get(cityCount) + "_" + cityName);
                }
            }
        });

        // 获取前二, 也就是获取treeMap的最后两个


        ArrayList<String> result = new ArrayList<>();


        // 封装前二

        Double sum = 0.0;

        while (treeMap.size() > 0 && result.size() < 2) {
            Map.Entry<Long, String> lastEntry = treeMap.lastEntry();
            Long cityCount = lastEntry.getKey();
            String cityNames = lastEntry.getValue();

            // 拼接
            // 北京21.2%，天津13.2%，其他65.6%
            double ratio = cityCount.doubleValue() * 100 / areaCount;
            String format = String.format("%.1f", ratio);


            // 天津_河北 30%
            String[] split = cityNames.split("_");
            for (String cityName : split) {
                sum += ratio;
                // 只保留两个城市
//                if (result.size() < 2) {
//                    result.add(cityName + format + "%");
//                }
                // 所有城市全都保留
                result.add(cityName + format + "%");
            }
            treeMap.remove(cityCount);
        }

        // 拼接其他城市
        double other = 100 - sum;
        String format = String.format("%.1f", other);

        result.add("其他" + format + "%");

        StringBuilder sb = new StringBuilder();
        for (String s : result) {
            sb.append(s).append(",");
        }
        return sb.substring(0, sb.length() -1);

//        return result.toString().substring(1, result.toString().length() - 1);
    }

    /*
     * 指定buffer类型
     * */
    @Override
    public Encoder<Buffer> bufferEncoder() {
        return Encoders.bean(Buffer.class);
    }

    /*
     * 指定输出类型
     * */
    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }


    @Data
    public static class Buffer implements Serializable {

        private Long areaCount;
        private Map<String, Long> citysCount;

        public Buffer() {
        }

        public Buffer(Long areaCount, Map<String, Long> citysCount) {
            this.areaCount = areaCount;
            this.citysCount = citysCount;
        }
    }
}
