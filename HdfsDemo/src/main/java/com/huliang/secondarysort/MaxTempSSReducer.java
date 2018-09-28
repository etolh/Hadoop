package com.huliang.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce任务对shuffle的<combokey(1978, 30), NullWriteable>先进行排序，再分组
 * reduce接收：同一year的combokey为同一组
 * 处理同一组有序的键值对集合，取第一个即为当前year的最大气温。
 * @author huliang
 * @date 2018/9/28 17:11
 */
public class MaxTempSSReducer extends Reducer<ComboKey, NullWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        // 无需迭代，每组的第一个key即是该年year与最大温度的组合
        int year = key.getYear();
        int temp = key.getTemp();

        System.out.println("==============>reduce");
        for(NullWritable v : values){
            System.out.println(key.getYear() + " : " + key.getTemp());
        }

        context.write(new IntWritable(year), new IntWritable(temp));

    }
}
