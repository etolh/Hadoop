package com.huliang.WCSkew;

import com.huliang.mr.MRInfoUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计单词数的Map
 * 输入：<1, "aa aaa aa ... aa"> 输出: <"aa", 1> <"aaa", 1>
 * @author huliang
 * @date 2018/9/28
 */
public class WCSkewMapper extends Mapper<LongWritable, Text, Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        String lines = value.toString();    // 输入行

        for(String word : lines.split("\\s+")) {
            try {
                context.write(new Text(word), new IntWritable(1));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
