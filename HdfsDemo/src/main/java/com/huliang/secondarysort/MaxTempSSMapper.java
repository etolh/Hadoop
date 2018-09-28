package com.huliang.secondarysort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Max Temperature Second sort Mapper
 * 输入：<offset,"1978 30">  输出：<combokey(1978, 30), NullWriteable>
 * @author huliang
 * @date 2018/9/28 17:11
 */
public class MaxTempSSMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable> {

    @Override
    // 抽取每行"1978 30",形成ComboKey输出
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arr = line.split("\\s+");
        int year = Integer.parseInt(arr[0]);
        int temp = Integer.parseInt(arr[1]);
        ComboKey comboKey = new ComboKey();
        comboKey.setYear(year);
        comboKey.setTemp(temp);

        System.out.println("MaxTempSSMapper:"+year+":"+temp);
        context.write(comboKey, NullWritable.get());

    }

}
