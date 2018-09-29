package com.huliang.wcmultiInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 多种输入格式MR: sequenceFile、TextFile
 *
 * @author huliang
 * @date 2018/9/27
 */
public class WCApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 1) {
            System.err.println("Usage: WCAppMulti <output path>");
            System.exit(-1);
        }

        // 本地运行MR 必备
        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///"); //本地MR必备，表示读取本地文件

        Job job = Job.getInstance(conf);
        job.setJarByClass(WCApp.class); //搜索类
        job.setJobName("Word Count With MultiInputFormat");    //作业名称

        // 多个输入格式 不同格式输入对应不同格式Mapper
        // Text输入key类型为LongWritable，Sequence输入key类型为IntWritable
        MultipleInputs.addInputPath(job, new Path("F:\\bigdata\\test\\multiInputs\\text"), TextInputFormat.class, WCTextMapper.class);
        MultipleInputs.addInputPath(job, new Path("F:\\bigdata\\test\\multiInputs\\seq"), SequenceFileInputFormat.class, WCSeqMapper.class);

        //添加输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

//        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setNumReduceTasks(2);       //设置reduce任务个数

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);    // 执行MR任务
    }
}
