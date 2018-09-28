package com.huliang.maxtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;

import java.io.IOException;

/**
 * 计算每年最大温度
 * @author huliang
 * @date 2018/9/27
 */
public class MaxTempApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7");

        if(args.length != 2) {
            System.err.println("Usage: MaxTempApp <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///"); //本地MR必备

        Job job = Job.getInstance(conf);       // 将conf进行拷贝JobConf jobConf = new JobConf(conf)，再创建job
        job.setJarByClass(MaxTempApp.class); //搜索类
        job.setJobName("Max Temperature");    //作业名称
//        job.setInputFormatClass(TextInputFormat.class); //设置输入格式
        job.setInputFormatClass(SequenceFileInputFormat.class); // 全排序格式，序列文件输入

        //添加输入路径、输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Mapper、Reducer以及格式设置
//        job.setMapperClass(MaxTempMapper.class);
        job.setMapperClass(MaxTempAllSortMapper.class); // 序列文件输入，其(k,v)就是(year,temp)
        job.setReducerClass(MaxTempReducer.class);
        job.setNumReduceTasks(3);       //设置reduce任务个数

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setPartitionerClass(MaxTempPartitioner.class);       // 设置自定义分区类
        job.setPartitionerClass(TotalOrderPartitioner.class);

        // 创建随机采样器对象
        // freq:每个key被选中的概率
        // numSapmple:抽取样本的总数
        // maxSplitSampled:最大采样切片数,即分区数，和reduce任务数一致
        InputSampler.Sampler<IntWritable, IntWritable> sampler =
                new InputSampler.RandomSampler<IntWritable, IntWritable>(0.5, 5000, 3);

        // 将sample写入分区文件
        // 获取job的创建conf，而非最初的conf
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("F:\\bigdata\\test\\maxtemp\\par.lst"));

        InputSampler.writePartitionFile(job, sampler);

        job.waitForCompletion(true);
    }
}
