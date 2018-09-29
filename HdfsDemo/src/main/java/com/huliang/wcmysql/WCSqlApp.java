package com.huliang.wcmysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import java.io.IOException;

/**
 *  单词计数 Mysql数据库读取/写入
 *  编写表映射类 WordDBWritable implements DBWritable,Writable
 *  配置数据库连接属性
 *
 * @author huliang
 * @date 2018/8/28 16:38
 */
public class WCSqlApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        System.setProperty("hadoop.home.dir", "F:\\iso\\linux-tars\\hadoop-2.7.7");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFs", "file:///");   //本地MR必备
        Job job = Job.getInstance(conf);

        String driverClass = "com.mysql.jdbc.Driver";
//        String url = "jdbc:mysql://localhost:3306/bigdata";
        String url = "jdbc:mysql://192.168.44.1:3306/bigdata";  // 集群配置
        String username = "root";
        String passwd = "4854264";

        // 数据库连接配置,并注册在job的conf中
        DBConfiguration.configureDB(job.getConfiguration(), driverClass, url, username, passwd);
        // 配置数据库读取内容: 映射类 查询语句 查询返回个数
        DBInputFormat.setInput(job, WordDBWritable.class, "select id, word from words", "select count(*) from words");
        // 配置数据库写入内容 : 表名 要写入的列(word count)
        DBOutputFormat.setOutput(job, "wordcount", "word", "count");
        job.setJarByClass(WCSqlApp.class);         //搜索类
        job.setJobName("Word Count");           //作业名称

        //添加输出路径 DB输出
//        FileOutputFormat.setOutputPath(job, new Path("F:\\bigdata\\test\\mysql\\out"));

        job.setMapperClass(WCSqlMapper.class);
        job.setReducerClass(WCSqlReducer.class);
        job.setNumReduceTasks(2);               //设置reduce任务个数

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
