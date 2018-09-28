package com.huliang.WCSkew;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

/**
 * 随机分区，使得数据均匀分布
 * @author huliang
 * @date 2018/9/28
 */
public class MySkewPartitioner extends Partitioner<Text, IntWritable> {

    public int getPartition(Text text, IntWritable intWritable, int num) {
        return new Random().nextInt(num);   // 随机
    }
}
