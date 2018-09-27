package com.huliang.maxtemp;

import com.huliang.mr.MRInfoUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 最大温度
 * 输入：<1, "1979 78">  输出: <1979, 78>
 * @author huliang
 * @date 2018/8/28 16:20
 */
public class MaxTempMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        String line = value.toString();    // 文本行
        String[] args = line.split("\\s+");   //空格划分
        int year = Integer.parseInt(args[0]);
        int temp = Integer.parseInt(args[1]);

        try {
            context.write(new IntWritable(year), new IntWritable(temp));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
