package com.wolffy.spark.sparkcore.Top10;


import com.wolffy.spark.sparkcore.bean.CategoryCountInfo;
import com.wolffy.spark.sparkcore.bean.UserVisitAction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class Test01_Top10_groupBy {

    public static void main(String[] args) throws InterruptedException {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[2]");

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码
        JavaRDD<String> lineRDD = sc.textFile("./sparkcore/src/main/resources/input/user_visit_action.txt");


        JavaRDD<UserVisitAction> map = lineRDD.map(new Function<String, UserVisitAction>() {
            @Override
            public UserVisitAction call(String line) throws Exception {
                String[] split = line.split("_");
                return new UserVisitAction(
                        split[0], //
                        split[1],
                        split[2],
                        split[3],
                        split[4],
                        split[5],
                        split[6],
                        split[7],
                        split[8],
                        split[9],
                        split[10],
                        split[11],
                        split[12]
                );
            }
        });
//        map.take(100).forEach(System.out::println);

        // UserVisitAction -> CategoryCountInfo
        JavaRDD<CategoryCountInfo> flatMap = map
                .flatMap(new FlatMapFunction<UserVisitAction, CategoryCountInfo>() {
                    @Override
                    public Iterator<CategoryCountInfo> call(UserVisitAction userVisitAction) throws Exception {
                        ArrayList<CategoryCountInfo> result = new ArrayList<>();

                        if (!"-1".equals(userVisitAction.getClick_category_id())) {
                            //点击日志
                            result.add(new CategoryCountInfo(userVisitAction.getClick_category_id(), 1L, 0L, 0L));
                        } else if (!"null".equals(userVisitAction.getOrder_category_ids())) {
                            // 下单日志
                            String[] ids = userVisitAction.getOrder_category_ids().split(",");
                            for (String id : ids) {
                                result.add(new CategoryCountInfo(id, 0L, 1L, 0L));
                            }
                        } else if (!"null".equals(userVisitAction.getPay_category_ids())) {
                            // 支付日志
                            String[] ids = userVisitAction.getPay_category_ids().split(",");
                            for (String id : ids) {
                                result.add(new CategoryCountInfo(id, 0L, 0L, 1L));
                            }
                        }
                        return result.iterator();
                    }
                });

        JavaPairRDD<String, Iterable<CategoryCountInfo>> groupBy = flatMap.groupBy(new Function<CategoryCountInfo, String>() {
            @Override
            public String call(CategoryCountInfo v1) throws Exception {
                return v1.getCategoryId();
            }
        });

        JavaRDD<CategoryCountInfo> mapCount = groupBy.map(new Function<Tuple2<String, Iterable<CategoryCountInfo>>, CategoryCountInfo>() {
            @Override
            public CategoryCountInfo call(Tuple2<String, Iterable<CategoryCountInfo>> v1) throws Exception {
                CategoryCountInfo sum = new CategoryCountInfo(v1._1, 0L, 0L, 0L);
                for (CategoryCountInfo elem : v1._2) {
                    sum.setClickCount(sum.getClickCount() + elem.getClickCount());
                    sum.setOrderCount(sum.getOrderCount() + elem.getOrderCount());
                    sum.setPayCount(sum.getPayCount() + elem.getPayCount());
                }
                return sum;
            }
        });

        JavaRDD<CategoryCountInfo> sortBy = mapCount.sortBy(new Function<CategoryCountInfo, CategoryCountInfo>() {
            @Override
            public CategoryCountInfo call(CategoryCountInfo v1) throws Exception {
                return v1;
            }
        }, false, 2);

        sortBy.take(10).forEach(System.out::println);
        Thread.sleep(999999);

        // 4 关闭资源
        sc.stop();
    }
}
