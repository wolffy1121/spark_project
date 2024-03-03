package com.wolffy.spark.sparkcore.Top10;


import com.wolffy.spark.sparkcore.bean.CategoryCountInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

public class Test03_Top10_Lambda {
    public static void main(String[] args) {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码
        sc
                .textFile("./src/main/resources/input/user_visit_action.txt")
                .flatMapToPair(line -> {
                    String[] split = line.split("_");
                    String clickId = split[6];
                    String orderIds = split[8];
                    String payIds = split[10];

                    ArrayList<Tuple2<String, CategoryCountInfo>> result = new ArrayList<>();

                    if (!"-1".equals(clickId)) {
                        // 点击日志
                        result.add(new Tuple2<>(clickId, new CategoryCountInfo(clickId, 1L, 0L, 0L)));
                    } else if (!"null".equals(orderIds)) {
                        // 下单日志
                        String[] ids = orderIds.split(",");
                        for (String id : ids) {
                            result.add(new Tuple2<>(id, new CategoryCountInfo(id, 0L, 1L, 0L)));
                        }
                    } else if (!"null".equals(payIds)) {
                        String[] ids = payIds.split(",");
                        for (String id : ids) {
                            result.add(new Tuple2<>(id, new CategoryCountInfo(id, 0L, 0L, 1L)));
                        }
                    }
                    return result.iterator();

                })
                .reduceByKey((sum, elem) -> {
                    sum.setClickCount(sum.getClickCount() + elem.getClickCount());
                    sum.setOrderCount(sum.getOrderCount() + elem.getOrderCount());
                    sum.setPayCount(sum.getPayCount() + elem.getPayCount());
                    return sum;
                })
                .map(v1 -> v1._2)
                .sortBy(countInfo -> countInfo, false, 2)
                .take(10)
                .forEach(System.out::println);
        // 4 关闭资源
        sc.stop();
    }
}
