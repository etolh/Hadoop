package com.huliang.mrjoin.simple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 内连接的MR实现：
 *  customers.txt(cid.cname,cage) orders.txt(oid,oname,price, cid)
 *  输出：(cid:cinfo / oid:oinfo)
 *
 *  customers.txt小文件，orders.txt大文件
 *  Maper直接读取小文件，对大文件进行map处理
 * @author huliang
 * @date 2018/10/2 16:48
 */
public class MRJoinApp1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");           //本地MR必备

        Job job = Job.getInstance(conf);
        job.setJarByClass(MRJoinApp1.class);            //搜索类
        job.setJobName("Inner Join By MR");             //作业名称
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        FileInputFormat.addInputPath(job, new Path("F:\\bigdata\\test\\mrjoin\\orders.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\bigdata\\test\\mrjoin\\out1"));

        job.setMapperClass(MRJoinMapper1.class);
        job.setNumReduceTasks(0);                       //设置reduce任务个数
        job.waitForCompletion(true);
    }
}
