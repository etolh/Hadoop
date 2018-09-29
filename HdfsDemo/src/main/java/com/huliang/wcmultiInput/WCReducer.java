package com.huliang.wcmultiInput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计reduce：
 * 输入： <"aa", <1,1,1> > 输出：<"aa",3>
 * @author huliang
 * @date 2018/8/28 16:32
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable count : values) {
            sum += count.get(); // 汇总
        }
        // 打印当前进程
        String tno = Thread.currentThread().getName();
        System.out.println(tno + "\t : MaxTempReducer :" + key.toString() + "=" + sum);
        context.write(key, new IntWritable(sum));
    }
}
