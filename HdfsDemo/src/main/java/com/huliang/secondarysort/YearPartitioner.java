package com.huliang.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 对map输出<combokey(1978, 30), NullWriteable> 进行分区，使得combokey中同一year的划分到同一区
 * @author huliang
 * @date 2018/9/28 17:29
 */
public class YearPartitioner extends Partitioner<ComboKey, NullWritable> {

    public int getPartition(ComboKey comboKey, NullWritable nullWritable, int num) {
        int year = comboKey.getYear();
        System.out.println("YearPartitioner:"+year+":"+year % num);
        return year % num;  // 简单取余划分
    }
}
