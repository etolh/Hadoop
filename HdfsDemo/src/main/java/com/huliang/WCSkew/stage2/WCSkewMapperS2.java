package com.huliang.WCSkew.stage2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读取首次MR的输出结果
 * 输入：(1, "hello	11") 分隔符为/t   输出:("hello"，11)
 *
 * @author huliang
 * @date 2018/9/28
 */
public class WCSkewMapperS2 extends Mapper<LongWritable, Text, Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        String line = value.toString();    // 输入行
        String[] arrs = line.split("\t");   // 分隔符为/t

        try {
            context.write(new Text(arrs[0]), new IntWritable(Integer.parseInt(arrs[1])));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
