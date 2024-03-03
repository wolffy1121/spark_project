package com.wolffy.spark.sparkcore.serializable;


import com.wolffy.spark.sparkcore.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Test02_KryoSerializer {
    public static void main(String[] args) throws ClassNotFoundException {
        // 1 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkCore")
                .setMaster("local[2]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 替换默认的序列化机制
                .registerKryoClasses(
                        new Class[]{
                                Class.forName("com.wolffy.spark.sparkcore.bean.User")
                        }
                )  // 注册需要使用kryo序列化的自定义类

        ;

        // 2 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 3 编写代码
        JavaRDD<String> lineRDD = sc.textFile("./sparkcore/src/main/resources/input/user.txt");

        // 1 zhangsan 18   ->  1, zhangsan, 18
        JavaRDD<User> map = lineRDD.map(new Function<String, User>() {
            @Override
            public User call(String line) throws Exception {
                String[] split = line.split(" ");
                if (split.length == 3) {
                    return new User(
                            Integer.valueOf(split[0]),
                            split[1],
                            Integer.valueOf(split[2])
                    );
                } else {
                    // 如果字符串为空，则使用默认值
                    return new User(
                            0, // id 默认为 0
                            "", // name 默认为空字符串
                            0 // age 默认为 0
                    );
                }
            }
        });

        JavaPairRDD<Integer, Iterable<User>> groupBy = map.groupBy(new Function<User, Integer>() {
            @Override
            public Integer call(User v1) throws Exception {
                return v1.getAge();
            }
        });

        groupBy.collect().forEach(System.out::println);


        // 4 关闭资源
        sc.stop();
    }
}
