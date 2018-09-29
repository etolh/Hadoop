package com.huliang.wcmysql;

import com.huliang.mr.MRInfoUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *  单词统计：数据库写入 输出格式 <K  extends DBWritable, V>
 *  由于statement只对K操作，V一般为NullWritable
 *
 *  输入： <"aa", <1,1,1> > 输出：<CountDBWritable("aa",3), Null>
 * @author huliang
 * @date 2018/9/29
 */
public class WCSqlReducer extends Reducer<Text, IntWritable, CountDBWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable count : values) {
            sum += count.get(); // 汇总
        }

        CountDBWritable ct = new CountDBWritable();
        ct.setWord(key.toString());
        ct.setCount(sum);
        context.write(ct, NullWritable.get());

    }
}
