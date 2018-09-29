package com.huliang.wcmultiInput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author huliang
 * @date 2018/9/27 18:16
 */
public class WCSeqMapper extends Mapper<IntWritable, Text, Text, IntWritable> {

    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        for(String word: line.split("\\s+")) {
            context.write(new Text(word), new IntWritable(1)); // 计数
        }

    }

}
