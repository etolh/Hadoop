package com.huliang.WCSkew.stage2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 二次MR 单词统计reduce 不变
 * 输入： <"aa", <1,1,1> > 输出：<"aa",3>
 * @author huliang
 * @date 2018/9/28
 */
public class WCSkewReducerS2 extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable count : values) {
            sum += count.get(); // 汇总
        }
        context.write(key, new IntWritable(sum));
    }
}
