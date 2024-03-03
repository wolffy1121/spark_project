package com.wolffy.spark.structured.streming.option;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class Test_08_TypeOpt {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RateSource")
                .getOrCreate();


        StructType peopleSchema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("sex", DataTypes.StringType);

        Dataset<Row> peopleDF = spark.readStream()
                .schema(peopleSchema)
                .json("src/main/resources");// 等价于: format("json").load(path)

        Dataset<String> stringDataset = peopleDF.as(ExpressionEncoder.javaBean(Person.class))
                .filter(new FilterFunction<Person>() {
                    @Override
                    public boolean call(Person value) throws Exception {
                        return value.getAge() > 20;
                    }
                })
                .map(new MapFunction<Person, String>() {
                    @Override
                    public String call(Person value) throws Exception {
                        return value.getName();
                    }
                }, Encoders.STRING());

        stringDataset.writeStream()
                .outputMode("update")
                .format("console")
                .start()
                .awaitTermination();

    }
}


