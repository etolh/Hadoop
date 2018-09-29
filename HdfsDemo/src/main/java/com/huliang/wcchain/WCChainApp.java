package com.huliang.wcchain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词计数App 链式MR：实现单词过滤
 * map1(切割单词) -> map2(过滤非法单词) -> reduce(统计) -> rmap1(过滤少数单词<3)
 *
 * @author huliang
 * @date 2018/9/29
 */
public class WCChainApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WCChainApp.class); //搜索类
        job.setJobName("Word Count");    //作业名称
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        //添加输入路径、输出路径
        FileInputFormat.addInputPath(job, new Path("F:\\bigdata\\test\\chain"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\bigdata\\test\\chain\\out"));

//        job.setMapperClass(WCChainMapper1.class);
//        job.setReducerClass(WCChainReducer.class);
        // 在mapper端增加mapper1、mapper2
        ChainMapper.addMapper(job, WCChainMapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
        ChainMapper.addMapper(job, WCChainMapper2.class,  Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        // 在reduce端增加reduce、reducemapper
        ChainReducer.setReducer(job, WCChainReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
        ChainReducer.addMapper(job, WCChainReduceMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
        job.setNumReduceTasks(3);       //设置reduce任务个数
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

    }
}
