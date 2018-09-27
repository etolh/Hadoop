package com.huliang.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 最大温度
 * 输入： <1978, (78, 10, -5) > 输出：<1978,78>
 * @author huliang
 * @date 2018/9/27
 */
public class MaxTempReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable year, Iterable<IntWritable> temperatures, Context context) throws IOException, InterruptedException {

        int max_temp = Integer.MIN_VALUE;    // 最小值

        for(IntWritable temp : temperatures) {
            max_temp = Math.max(max_temp, temp.get());  // 获取最大温度
        }
        context.write(year, new IntWritable(max_temp));
    }

}
