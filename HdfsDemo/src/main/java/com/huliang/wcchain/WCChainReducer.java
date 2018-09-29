package com.huliang.wcchain;

import com.huliang.mr.MRInfoUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计reduce：
 * 输入： <"aa", <1,1,1> > 输出：<"aa",3>
 * @author huliang
 * @date 2018/9/29
 */
public class WCChainReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable count : values) {
            sum += count.get(); // 汇总
        }
        context.write(key, new IntWritable(sum));
    }
}
