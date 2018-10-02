package com.huliang.mrjoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 对map输出结果进行分区：同cid 同区
 * @author huliang
 * @date 2018/10/2 18:11
 */
public class CusOrdKeyPartitioner extends Partitioner<CusOrdKey, NullWritable> {

    public int getPartition(CusOrdKey cusOrdKey, NullWritable nullWritable, int numPartitions) {
        return cusOrdKey.getCid() % numPartitions;
    }
}
