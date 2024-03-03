package com.wolffy.spark.sparkcore.partition;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {

    private int numPartitions;



    @Override
    public int numPartitions() {
        return numPartitions;
    }

    /*
    * 传入参数 ：数据的key
    * return ： 分区号1
    * */
    @Override
    public int getPartition(Object key) {
        int intKey = (int) key;

        if (intKey == 1) {
            return 0;
        } else if (intKey == 2) {
            return 1;
        } else {
            return intKey % numPartitions();
        }
    }



    public CustomPartitioner() {
    }

    public CustomPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }
}
