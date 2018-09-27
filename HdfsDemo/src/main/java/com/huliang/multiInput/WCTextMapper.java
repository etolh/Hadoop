package com.huliang.multiInput;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author huliang
 * @date 2018/9/27 18:13
 */
public class WCTextMapper extends Mapper<LongWritable, Text, Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        String line = value.toString();    // 行文本，切词

        // 打印当前进程
        String tno = Thread.currentThread().getName();
        System.out.println(tno + "\tWCTextMapper\t" +"value:"+ value.toString());

        for(String word : line.split("\\s+")) {
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
