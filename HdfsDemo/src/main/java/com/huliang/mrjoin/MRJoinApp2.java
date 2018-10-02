package com.huliang.mrjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author huliang
 * @date 2018/10/2 17:06
 */
public class MRJoinApp2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");           //本地MR必备

        Job job = Job.getInstance(conf);
        job.setJarByClass(MRJoinApp2.class);            //搜索类
        job.setJobName("Inner Join By MR 2 Big Text");  //作业名称
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        // customers.txt orders.txt 都经过map、reduce处理
        FileInputFormat.addInputPath(job, new Path("F:\\bigdata\\test\\mrjoin"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\bigdata\\test\\mrjoin\\out2"));

        job.setMapperClass(MRJoinMapper2.class);
        job.setReducerClass(MRJoinReducer2.class);

        // 设置Map、Reduce输出类型
        job.setMapOutputKeyClass(CusOrdKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 分区、reduce排序分组设置
        job.setPartitionerClass(CusOrdKeyPartitioner.class);             // 分区
        job.setSortComparatorClass(CusOrdSortComparator.class);         // 排序
        job.setGroupingComparatorClass(CusOrdKeyGroupComparator.class); // 分组

        job.setNumReduceTasks(3);                       //设置reduce任务个数
        job.waitForCompletion(true);
    }
}
