package com.wolffy.spark.structured.streming.window;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;


import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Test_10_Window {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // 创建一个SparkSession对象，用于与Apache Spark进行交互和分析
        SparkSession spark = SparkSession
                .builder()
                .appName("window-demo")
                .master("local[*]")
                .getOrCreate();
        // 以socket方式从指定主机和端口读取数据，并将数据转换为DataFrame，同时给数据添加时间戳。
        Dataset<Row> lines = spark.readStream()
                .format("socket") // 设置数据源
                .option("host", "localhost")
                .option("port", 9999)
                // 给产生的数据自动添加时间戳
                .option("includeTimestamp", true)
                .load();
        // (a,a,a,),2024-01-01)
        // lines中的元素转换为元组，每个元组的第一个元素是字符串类型，第二个元素是时间戳类型。使用Encoders来指定元素的编码方式。
        Dataset<Tuple2<String, Timestamp>> inputLines = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()));

        // RowDataset转换为元组格式，其中包含两个元素：一个字符串类型和一个时间戳类型。
        // ((a,a,a,),2024-01-01)---->(a,2024-01-01),
        Dataset<Tuple2<String, Timestamp>> wordsWithTs = inputLines.flatMap(
                new FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>() {
                    @Override
                    public Iterator<Tuple2<String, Timestamp>> call(Tuple2<String, Timestamp> tuple) throws Exception {
                        // 正则表达式 "\W+" 匹配一个或多个非单词字符。
                        String[] words = tuple._1.split("\\W+");
                        Timestamp timestamp = tuple._2;
                        List<Tuple2<String, Timestamp>> outputList = new ArrayList<>();

                        for (String word : words) {
                            outputList.add(new Tuple2<>(word, timestamp));
                        }
                        return outputList.iterator();
                    }

                },
                Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()));

        // 函数将一个包含单词和时间戳的数据集转换为DataFrame，其中DataFrame的列包括"word"和"timestamp"。
        Dataset<Row> words = wordsWithTs.toDF("word", "timestamp");

        // 将"word"列和"timestamp"列按时间窗口划分数据，同时按单词内容对数据进行分组。
        // 例如，对于时间窗口(2024-01-01,2024-01-02)，如果"word"列的值为"a"，则该行数据将被添加到时间窗口(a,2024-01-01)中。如果"word"列的值为"b"，则该行数据将被添加到时间窗口(b,2024-01-01)中。
        Dataset<Row> windowedCounts = words.groupBy(
                        // 对时间窗口分组。这个窗口的大小为4分钟，滑动步长为2分钟，即每次窗口向前滑动2分钟，
                        functions.window(words.col("timestamp"), "4 minutes", "2 minutes"),
                        // 还按照"word"列进行分组，按时间窗口划分数据，还会进一步在每个时间窗口内按照单词内容细分。
                        words.col("word"))
                // 对满足同一组条件的数据进行计数操作，得到的结果是一个新的Dataset<Row>，
                // 其中每一行表示一个特定的时间窗口和单词组合及其在该窗口内出现的次数。
                .count()
                .orderBy("window");

        wordsWithTs.writeStream()
                .format("console")
                .outputMode("complete")
                .option("truncate", false)
                .start()
                .awaitTermination();

    }
}