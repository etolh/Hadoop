package com.huliang.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计单词数的Map
 * 输入：<1, "aa aaa aa ... aa"> 输出: <"aa", 1> <"aaa", 1>
 * @author huliang
 * @date 2018/8/28 16:20
 */
public class WCMapper extends Mapper<LongWritable, Text, Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) {

        String lines = value.toString();    // 输入行

        // 打印当前进程
//        String tno = Thread.currentThread().getName();
//        System.out.println(tno + "\tWCMapper\t" +"value:"+ value.toString());

        for(String word : lines.split("\\s+")) {
            try {
                context.write(new Text(word), new IntWritable(1));
                // 设置group和counter
                context.getCounter("m", MRInfoUtil.info(this, "map")).increment(1L);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
