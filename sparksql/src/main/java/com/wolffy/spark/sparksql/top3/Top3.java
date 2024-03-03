package com.wolffy.spark.sparksql.top3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.udaf;

public class Top3 {
    public static void main(String[] args) throws AnalysisException {
        // 1 创建spark配置
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]");

        // 2 创建sparksession
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();


        spark.udf().register("cityMark", udaf(new CityMark(), Encoders.STRING()));

        // 3 编写代码
        spark.sql("select\n" +
                "\tc.area,\n" +
                "\tp.product_name,\n" +
                "\tc.city_name\n" +
                "from\n" +
                "\tuser_visit_action u\n" +
                "join\n" +
                "\tcity_info c\n" +
                "on\n" +
                "\tu.city_id = c.city_id\n" +
                "join\n" +
                "\tproduct_info p\n" +
                "on u.click_product_id = p.product_id\n" +
                "where\n" +
                "\tu.click_product_id != -1")
                .createTempView("t1");
        spark.sql("select\n" +
                "\tt1.area,\n" +
                "\tt1.product_name,\n" +
                "\tcount(*) click_count,\n" +
                "\tcityMark(t1.city_name) city_mark\n" +
                "from \n" +
                "\tt1\n" +
                "group by\n" +
                "\tt1.area,\n" +
                "\tt1.product_name")
                .createTempView("t2");

        spark.sql("select\n" +
                "\tt2.area,\n" +
                "\tt2.product_name,\n" +
                "\tt2.click_count,\n" +
                "\trank() over(partition by t2.area order by t2.click_count desc) rk,\n" +
                "\tt2.city_mark\n" +
                "from\n" +
                "\tt2")
                .createTempView("t3");

        spark.sql("select\n" +
                "\tt3.area,\n" +
                "\tt3.product_name,\n" +
                "\tt3.click_count,\n" +
                "\tt3.rk,\n" +
                "\tt3.city_mark\n" +
                "from\n" +
                "\tt3\n" +
                "where\n" +
                "\tt3.rk <= 3")
                .show(false);

        // 4 关闭资源
        spark.close();
    }
}
