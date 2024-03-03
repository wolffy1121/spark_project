package com.wolffy.spark.sparksql.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class T2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("spar-ksql")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> json =
                spark.read().json("./sparksql/src/main/resources/input/user_visit_action.json");
        json.show();


        spark.close();

    }
}
