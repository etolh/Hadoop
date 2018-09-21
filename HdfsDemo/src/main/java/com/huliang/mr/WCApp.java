package com.huliang.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词计数App:
 * @author huliang
 * @date 2018/8/28 16:38
 */
public class WCApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行MR 必备
        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7");

        if(args.length != 2) {
            System.err.println("Usage: WCApp <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///"); //本地MR必备

        Job job = Job.getInstance(conf);
        job.setJarByClass(WCApp.class); //搜索类
        job.setJobName("Word Count");    //作业名称
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        //添加输入路径、输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        FileInputFormat.setMaxInputSplitSize(job, 13L); // 设置最大切片大小
//        FileInputFormat.setMinInputSplitSize(job, 1L);  // 设置最小切片大小

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
