package com.huliang.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 最大温度 二次排序
 * @author huliang
 * @date 2018/9/28 17:10
 */
public class MaxTempSSApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 2) {
            System.err.println("Usage: MaxTempApp <input path> <output path>");
            System.exit(-1);
        }

        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7"); // 本地hadoop
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///"); //本地MR必备
        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxTempSSApp.class);
        job.setJobName("Max Temperature With Secondary Sort");

        job.setInputFormatClass(TextInputFormat.class); //文本输入
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTempSSMapper.class);
        job.setReducerClass(MaxTempSSReducer.class);
        // Map输出类型
        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        // Reduce输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(YearPartitioner.class);     // 自定义分区类
        job.setSortComparatorClass(ComboKeyComparator.class);   //排序比较器
        job.setGroupingComparatorClass(YearGroupComparator.class); // 分组比较器

        job.setNumReduceTasks(3);

        job.waitForCompletion(true);
    }
}
