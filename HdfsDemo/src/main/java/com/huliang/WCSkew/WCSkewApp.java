package com.huliang.WCSkew;

import com.huliang.mr.MyPartitioner;
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
 *  单词计数App: 数据倾斜       随机分区 + 二次MR
 *  随机分区使得原本倾斜的数据较均匀地分到各个Reducer任务中
 *  再对Reducer输出结果进行二次MR运算，将分布到多个reduce的同一单词综合起来
 *
 * @author huliang
 * @date 2018/9/28
 */
public class WCSkewApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行MR 必备
//        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7");

        if(args.length != 2) {
            System.err.println("Usage: MaxTempApp <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");           //本地MR必备

        Job job = Job.getInstance(conf);
        job.setJarByClass(WCSkewApp.class);             //搜索类
        job.setJobName("Word Count With Data Skew");                   //作业名称
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        //添加输入路径、输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setPartitionerClass(MySkewPartitioner.class);           //设置分区类
//        job.setCombinerClass(WCSkewReducerS2.class);              //设置combiner类

        job.setMapperClass(WCSkewMapper.class);
        job.setReducerClass(WCSkewReducer.class);
        job.setNumReduceTasks(3);       //设置reduce任务个数

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
