package com.huliang.wcchain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计单词数Map : 过滤非法单词
 * @author huliang
 * @date 2018/9/29
 */
public class WCChainMapper2 extends Mapper<Text, IntWritable, Text,IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) {
        String skey = key.toString();
        if(!skey.contains("hal")) {
            try {
                context.write(key, value);  // 无非法词，才输出
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
