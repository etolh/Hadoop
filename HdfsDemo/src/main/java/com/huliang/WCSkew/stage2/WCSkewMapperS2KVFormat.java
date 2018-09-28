package com.huliang.WCSkew.stage2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读取首次MR的输出结果 KeyValueTextInputFormat格式 其读取格式默认为<Text, Text>
 * 输入：("hello",	"11")    输出:("hello"，11)
 *
 * @author huliang
 * @date 2018/9/28
 */
public class WCSkewMapperS2KVFormat extends Mapper<Text, Text, Text,IntWritable> {

    @Override
    protected void map(Text key, Text value, Context context) {
        // key，value即为输出的值，value需要格式转换
        try {
            context.write(key, new IntWritable(Integer.parseInt(value.toString())));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
