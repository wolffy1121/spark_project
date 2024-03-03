package com.wolffy.spark.sparksql.top3;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static org.apache.spark.sql.functions.udaf;


public class Top3_Cluster {
    public static void main(String[] args) throws AnalysisException {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("top3");
        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();
        spark.udf().register("cityMark", udaf(new CityMark(), Encoders.STRING()));

        // 3. 编写代码
        // spark.sql("use default");
//        spark.read().textFile(args[0]).createOrReplaceTempView("user_visit_action");
//        spark.read().textFile(args[1]).createOrReplaceTempView("city_info");
//        spark.read().textFile(args[2]).createOrReplaceTempView("product_info");


        // 将3个表格数据join在一起
        spark.sql("select c.area,c.city_name,p.product_name,v.click_product_id\n" +
                        "from user_visit_action v join city_info c on v.city_id = c.city_id join product_info p on v.click_product_id = p.product_id\n" +
                        "where click_product_id > -1")
                .createTempView("t1");
        spark.sql("select t1.area,t1.product_name,count(*) click_count,city_remark(t1.city_name) from t1 group by t1.area, t1.product_name")
                .createTempView("t2");

        spark.sql("select *,rank() over (partition by t2.area order by t2.click_count desc) rank from t2")
                .createTempView("t3");
        // 同时结果写出到表格中
        spark.sql("drop table if exists result");
        spark.sql("create table result as select * from t3 where rank <= 3");

        // 4. 关闭sparkSession
        spark.close();
    }
    @Data
    public static class Buffer implements Serializable {
        private Long totalCount;
        private HashMap<String, Long> map;

        public Buffer() {
        }

        public Buffer(Long totalCount, HashMap<String, Long> map) {
            this.totalCount = totalCount;
            this.map = map;
        }
    }
    public static class CityMark extends Aggregator<String, Buffer, String> {

        @Override
        public Buffer zero() {
            return new Buffer(0L, new HashMap<String, Long>());
        }

        /**
         * 分区内的预聚合
         *
         * @param b map(城市,sum)
         * @param a 当前行表示的城市
         * @return
         */
        @Override
        public Buffer reduce(Buffer b, String a) {
            HashMap<String, Long> hashMap = b.getMap();
            // 如果map中已经有当前城市  次数+1
            // 如果map中没有当前城市    0+1
            hashMap.put(a, hashMap.getOrDefault(a, 0L) + 1);

            b.setTotalCount(b.getTotalCount() + 1L);
            return b;
        }

        /**
         * 合并多个分区间的数据
         *
         * @param b1 (北京,100),(上海,200)
         * @param b2 (天津,100),(上海,200)
         * @return
         */
        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setTotalCount(b1.getTotalCount() + b2.getTotalCount());

            HashMap<String, Long> map1 = b1.getMap();
            HashMap<String, Long> map2 = b2.getMap();
            // 将map2中的数据放入合并到map1
            map2.forEach(new BiConsumer<String, Long>() {
                @Override
                public void accept(String s, Long aLong) {
                    map1.put(s, aLong + map1.getOrDefault(s, 0L));
                }
            });

            return b1;
        }

        /**
         * map => {(上海,200),(北京,100),(天津,300)}
         *
         * @param reduction
         * @return
         */
        @Override
        public String finish(Buffer reduction) {
            Long totalCount = reduction.getTotalCount();
            HashMap<String, Long> map = reduction.getMap();
            // 需要对map中的value次数进行排序
            TreeMap<Long, String> treeMap = new TreeMap<>();

            // 将map中的数据放入到treeMap中 进行排序
            map.forEach(new BiConsumer<String, Long>() {
                @Override
                public void accept(String s, Long aLong) {
                    if (treeMap.containsKey(aLong)) {
                        // 如果已经有当前值
                        treeMap.put(aLong, treeMap.get(aLong) + "_" + s);
                    } else {
                        // 没有当前值
                        treeMap.put(aLong, s);
                    }
                }
            });

            ArrayList<String> resultMark = new ArrayList<>();

            Double sum = 0.0;

            // 当前没有更多的城市数据  或者  已经找到两个城市数据了  停止循环
            while (!(treeMap.size() == 0) && resultMark.size() < 2) {
                String cities = treeMap.lastEntry().getValue();
                Long counts = treeMap.lastEntry().getKey();
                String[] strings = cities.split("_");
                for (String city : strings) {
                    double rate = counts.doubleValue() * 100 / totalCount;
                    sum += rate;
                    resultMark.add(city + String.format("%.2f", rate) + "%");
                }
                // 添加完成之后删除当前key
                treeMap.remove(counts);
            }

            // 拼接其他城市
            if (treeMap.size() > 0) {
                resultMark.add("其他" + String.format("%.2f", 100 - sum) + "%");
            }

            StringBuilder cityMark = new StringBuilder();
            for (String s : resultMark) {
                cityMark.append(s).append(",");
            }

            return cityMark.substring(0, cityMark.length() - 1);
        }

        @Override
        public Encoder<Buffer> bufferEncoder() {
            return Encoders.javaSerialization(Buffer.class);
        }

        @Override
        public Encoder<String> outputEncoder() {
            return Encoders.STRING();
        }
    }
}
