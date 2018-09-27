package com.huliang.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author huliang
 * @date 2018/9/27 21:40
 */
public class MaxTempPartitioner extends Partitioner<IntWritable, IntWritable> {

    /**
     * 根据key/year进行分区,num：分区个数
     * 根据数据范围平均分区，year: 1950-2049  100年分为num个
     * @param year
     * @param temp
     * @param num
     * @return
     */
    public int getPartition(IntWritable year, IntWritable temp, int num) {

        int width = 100 / num;  // 分区大小
        int range = year.get() - 1950;  // 越过范围
        int par =  range / width;
        if(par >= num)
            par = num - 1; // 当width未整除时，最后一个数超过分区数
        return  par;
    }
}
