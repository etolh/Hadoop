package com.huliang.wcmysql;

import com.huliang.mr.MRInfoUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计单词数Mapper: 从数据库读取 格式<LongWritable, T extends DBWritable>
 * 输入：<off, WordDB(id,"aa b")> 输出: <"aa", 1> <"b", 1>
 * @author huliang
 * @date 2018/9/29
 *
 */
public class WCSqlMapper extends Mapper<LongWritable, WordDBWritable, Text,IntWritable> {

    @Override
    protected void map(LongWritable key, WordDBWritable value, Context context) {

        String txt = value.getWord();   // 获取数据库文本
        // 切割统计
        for(String word: txt.split("\\s+")) {
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
