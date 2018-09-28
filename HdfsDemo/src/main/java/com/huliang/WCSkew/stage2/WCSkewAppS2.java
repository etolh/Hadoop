package com.huliang.WCSkew.stage2;

import com.huliang.WCSkew.MySkewPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *  单词计数App: 数据倾斜       随机分区 + 二次MR
 *
 *  二次MR : 对Reducer输出结果进行二次MR运算，将分布到多个reduce的同一单词综合起来
 *  由于Reducer输出结果为 key\tvalue 的格式，即可以采用TextInputFormat输入，也可以采用
 *
 * @author huliang
 * @date 2018/9/28
 */
public class WCSkewAppS2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行MR 必备
        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");           //本地MR必备

        Job job = Job.getInstance(conf);
        job.setJarByClass(WCSkewAppS2.class);             //搜索类
        job.setJobName("Word Count With Data Skew Stage2: Secondary MR");                   //作业名称
//        job.setInputFormatClass(TextInputFormat.class);     //设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 读取首次MR的输出结果
        FileInputFormat.addInputPath(job, new Path("F:\\bigdata\\test\\wcskew\\out\\part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("F:\\bigdata\\test\\wcskew\\out\\part-r-00001"));
        FileInputFormat.addInputPath(job, new Path("F:\\bigdata\\test\\wcskew\\out\\part-r-00002"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\bigdata\\test\\wcskew\\outkv"));

//        job.setMapperClass(WCSkewMapperS2.class);
        job.setMapperClass(WCSkewMapperS2KVFormat.class); // KV格式mapper
        job.setReducerClass(WCSkewReducerS2.class);
        job.setNumReduceTasks(3);       //设置reduce任务个数

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
