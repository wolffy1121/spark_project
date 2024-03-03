package com.wolffy.spark.sparkcore.serializable;

import com.wolffy.spark.sparkcore.bean.User0;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;


public class Test01_Serializer {
    public static void main(String[] args) {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        User0 zhangsan = new User0("zhangsan", 13);
        User0 lisi = new User0("lisi", 13);

        JavaRDD<User0> userJavaRDD = sc.parallelize(Arrays.asList(zhangsan, lisi), 2);

        JavaRDD<User0> mapRDD = userJavaRDD.map(new Function<User0, User0>() {
            @Override
            public User0 call(User0 v1) throws Exception {
                return new User0(v1.getName(), v1.getAge() + 1);
            }
        });

        mapRDD.collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
