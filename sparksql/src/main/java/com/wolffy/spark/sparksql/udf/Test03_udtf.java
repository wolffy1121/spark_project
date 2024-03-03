package com.wolffy.spark.sparksql.udf;


import com.wolffy.spark.sparksql.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;

/*
* 没有自定义的udtf函数
* 可以先用flatMap炸裂开之后， 创建个临时视图，再用sql处理
* */
public class Test03_udtf {

    public static void main(String[] args) throws AnalysisException {
        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]");

        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3 编写代码
        Dataset<User> dataset = spark.read().json("./src/main/resources/input/user.json").as(Encoders.bean(User.class));

        Dataset<User> flatMap = dataset.flatMap(new FlatMapFunction<User, User>() {
            @Override
            public Iterator<User> call(User user) throws Exception {
                ArrayList<User> users = new ArrayList<>();
                users.add(user);
                users.add(user);
                users.add(user);
                return users.iterator();
            }
        }, Encoders.bean(User.class));

        flatMap.createTempView("user_info");

        spark.sql("select * from user_info").show();

        // 4 关闭资源
        spark.close();
    }
}
