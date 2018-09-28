package com.huliang.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区函数，根据
 * @author huliang
 * @date 2018/9/25 15:11
 */
public class MyPartitioner extends Partitioner<Text, IntWritable> {

    public int getPartition(Text text, IntWritable intWritable, int i) {

        System.out.println("MySkewPartitioner\t"+"key: "+text.toString()+"\tvalue:"+intWritable.get());
        return 0;
    }
}
