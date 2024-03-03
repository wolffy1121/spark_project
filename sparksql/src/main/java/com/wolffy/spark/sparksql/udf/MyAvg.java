package com.wolffy.spark.sparksql.udf;

import lombok.Data;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/*
* IN : 传输的数据类型
* buffer ： 中间结果，类型比较灵活， 不是写死的类型， 需要我们根据需求自定义（例如avg = sum / count）
* OUT ： 传出的数据类型
* */
public class MyAvg extends Aggregator<Long, MyAvg.Buffer, Double> {
    /*
    * 初始化buffer
    * */
    @Override
    public Buffer zero() {
        return new Buffer(0L, 0L);
    }


    /*
    * 分区内聚合
    * */
    @Override
    public Buffer reduce(Buffer b, Long age) {
        b.setSum(b.getSum() + age);
        b.setCount(b.getCount() + 1);
        return b;
    }

    /*
     * 分区间聚合
     * 只能给b2累加到b1
     * */
    @Override
    public Buffer merge(Buffer b1, Buffer b2) {
        b1.setSum(b1.getSum() + b2.getSum());
        b1.setCount(b1.getCount() + b2.getCount());
        return b1;
    }


    /*
     * 计算最终结果 sum / count = avg
     * */
    @Override
    public Double finish(Buffer reduction) {

        return reduction.getSum().doubleValue() / reduction.getCount();
    }


    /*
     * 指定buffer类型
     * */
    @Override
    public Encoder<Buffer> bufferEncoder() {
        return Encoders.bean(Buffer.class);
    }

    /*
     * 指定输出类型
     * */
    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }

    @Data
    public static class Buffer implements Serializable {
        private Long sum;
        private Long count;

        public Buffer() {
        }

        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }
    }
}
