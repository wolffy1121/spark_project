package com.wolffy.spark.sparksql.top3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.udaf;

public class MyTop3 {
    public static void main(String[] args) {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("top3");
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

        spark.udf().register("cityMark", udaf(new CityMark(), Encoders.STRING()));

        spark.sql("select c.area,\n" +
                "       c.city_name,\n" +
                "       p.product_name,\n" +
                "       v.click_product_id\n" +
                "from user_visit_action v\n" +
                "         join city_info c\n" +
                "              on v.city_id = c.city_id\n" +
                "         join product_info p\n" +
                "              on v.click_product_id = p.product_id\n" +
                "where click_product_id > -1").createOrReplaceTempView("t1");
        //+----+---------+------------+----------------+
        //|area|city_name|product_name|click_product_id|
        //+----+---------+------------+----------------+
        //|华北|     保定|     商品_98|              98|
        //|华北|     保定|     商品_98|              98|
        //|华北|     保定|     商品_98|              98|
        //
        spark.sql("select t1.area,                  \n" +
                "       t1.product_name,          \n" +
                "       count(*) click_count,     \n" +
                "       cityMark(t1.city_name) \n" +
                "from t1\n" +
                "group by t1.area, t1.product_name").createOrReplaceTempView("t2");
        //+----+------------+-----------+-------------------------+
        //|area|product_name|click_count|      citymark(city_name)|
        //+----+------------+-----------+-------------------------+
        //|东北|      商品_1|       1040|大连38.5%,沈阳32.3%,其...|
        //|东北|     商品_10|       1168|大连37.0%,哈尔滨34.9%,...|
        //|东北|    商品_100|       1048|哈尔滨35.9%,沈阳33.6%,...|
        //|东北|     商品_11|       1096|哈尔滨39.4%,沈阳35.8%,...|
        //|东北|     商品_12|       1216|大连35.5%,哈尔滨33.6%,...|
        // ....

        spark.sql("select *,\n" +
                "       rank() over (partition by t2.area order by t2.click_count desc) rank \n" +
                "from t2;").createOrReplaceTempView("t3");
        // +----+------------+-----------+-------------------------+----+
        //|area|product_name|click_count|      citymark(city_name)|rank|
        //+----+------------+-----------+-------------------------+----+
        //|东北|     商品_41|       1352|哈尔滨35.5%,大连34.9%,...|   1|
        //|东北|     商品_91|       1320|哈尔滨35.8%,大连32.7%,...|   2|
        //|东北|     商品_58|       1272|沈阳37.7%,大连32.1%,其...|   3|
        //|东北|     商品_93|       1272|哈尔滨38.4%,大连37.1%,...|   3|
        //|东北|     商品_13|       1264|沈阳38.0%,哈尔滨32.3%,...|   5|

        spark.sql("select * from t3 where rank <= 3").show(false);

        //+----+------------+-----------+---------------------------------------+----+
        //|area|product_name|click_count|citymark(city_name)                    |rank|
        //+----+------------+-----------+---------------------------------------+----+
        //|东北|商品_41     |1352       |哈尔滨35.5%,大连34.9%,其他29.6%        |1   |
        //|东北|商品_91     |1320       |哈尔滨35.8%,大连32.7%,其他31.5%        |2   |
        //|东北|商品_58     |1272       |沈阳37.7%,大连32.1%,其他30.2%          |3   |
        //|东北|商品_93     |1272       |哈尔滨38.4%,大连37.1%,其他24.5%        |3   |
        //|华东|商品_86     |2968       |上海16.4%,杭州15.9%,无锡15.9%,其他51.8%|1   |
        //|华东|商品_47     |2928       |杭州15.8%,青岛15.6%,其他68.6%          |2   |
        //|华东|商品_75     |2928       |上海17.5%,无锡15.6%,其他66.9%          |2   |


        spark.close();
    }
}
