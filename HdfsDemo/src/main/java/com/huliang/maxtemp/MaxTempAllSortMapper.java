package com.huliang.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 最大温度
 * 输入：<1978, 78>  输出: <1979, 78>
 * @author huliang
 * @date 2018/8/28 16:20
 */
public class MaxTempAllSortMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void map(IntWritable year, IntWritable temp, Context context) {
        try {
            context.write(year, temp);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
