package com.wolffy.spark.sparksql.bean;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class Test01_Method {
    public static void main(String[] args) {

        //1. 创建配置对象
        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local[*]");

        //2. 获取sparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //3. 编写代码
        // 按照行读取
        Dataset<Row> lineDS = spark.read().json("./src/main/resources/input/user.json");

        // 转换为类和对象
        Dataset<User> userDS = lineDS.as(Encoders.bean(User.class));

//        userDS.show();

        // 使用方法操作
        // 函数式的方法
        Dataset<User> userDataset = lineDS
                .map(new MapFunction<Row, User>() {
                         @Override
                         public User call(Row value) throws Exception {
                             return new User(value.getLong(0), value.getString(1));
                         }
                     },
                        // 使用kryo在底层会有部分算子无法使用
                        Encoders.bean(User.class));

        // 常规方法
        Dataset<User> sortDS = userDataset.sort(new Column("age"));
        sortDS.show();

        // 区分
        RelationalGroupedDataset groupByDS = userDataset.groupBy("name");

        // 后续方法不同
        Dataset<Row> count = groupByDS.count();

        // 推荐使用函数式的方法  使用更灵活
        KeyValueGroupedDataset<String, User> groupedDataset = userDataset.groupByKey(new MapFunction<User, String>() {
            @Override
            public String call(User value) throws Exception {

                return value.name;
            }
        }, Encoders.STRING());

        // 聚合算子都是从groupByKey开始
        // 推荐使用reduceGroup
        Dataset<Tuple2<String, User>> result = groupedDataset.reduceGroups(new ReduceFunction<User>() {
            @Override
            public User call(User v1, User v2) throws Exception {
                // 取用户的大年龄
                return new User(Math.max(v1.age, v2.age), v1.name);
            }
        });

        result.show();

        //4. 关闭sparkSession
        spark.close();
    }
}
