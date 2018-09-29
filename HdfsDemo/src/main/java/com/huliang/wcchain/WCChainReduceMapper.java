package com.huliang.wcchain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * reduce阶段后的map：过滤少数单词 <3
 * @author huliang
 * @date 2018/9/29
 */
public class WCChainReduceMapper extends Mapper<Text, IntWritable, Text,IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) {
        if(value.get() >= 3) {
            try {
                context.write(key, value);  // >=3单词输出
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
