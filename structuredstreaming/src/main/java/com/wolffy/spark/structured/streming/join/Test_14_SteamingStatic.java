package com.wolffy.spark.structured.streming.join;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;


public class Test_14_SteamingStatic {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SteamingStatic")
                .getOrCreate();

        // 定义 Schema, 用于指定列名以及列中的数据类型
        StructType userSchema = new StructType()
                .add("name", DataTypes.StringType)
                .add("sex", DataTypes.StringType);

        Dataset<Row> staticDF = spark.readStream()
                .format("csv")
                .schema(userSchema)
                .load("src/main/resources/input");
        // 动态df
        Dataset<Row> steamingDF = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, Tuple2<String, String>>) line -> {
                    String[] splits = line.split(",");
                    return Arrays.asList(new Tuple2<>(splits[0], splits[1])).iterator();
                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .toDF("name", "sex");

        Dataset<Row> joinedDF = steamingDF
                .join(staticDF, "inner");

        // 开始处理流数据（这里仅做示例，实际应用中需要调用writeStream并启动查询）
        StreamingQuery query = joinedDF
                .writeStream()
                .outputMode("append")
                .format("console") // 或者选择其他输出格式
                .start();

        // 等待流处理结束
        query.awaitTermination();
    }


}
