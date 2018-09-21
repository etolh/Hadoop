package com.huliang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

import java.io.IOException;


/**
 * @author huliang
 * @date 2018/9/10 20:51
 */
public class TestSeqFile {

    /**
     * 写操作
     */
    @Test
    public void write() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");    //本地文件系统
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("F:/bigdata/test/seq/t.seq");


        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
        for(int i = 0; i < 10; i++) {
            writer.append(new IntWritable(i), new Text("tom" + i));
        }

        for(int i = 0; i < 10; i++) {
            if(i % 2 == 0)
                writer.sync();  //每隔一个位置插入一个同步点
            writer.append(new IntWritable(i), new Text("tom" + i));
        }

        writer.close();

    }

    /**
     * 写操作：带压缩格式
     */
    @Test
    public void write2() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");    //本地文件系统
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("F:/bigdata/test/seq/compress_t.seq");


        SequenceFile.Writer writer = SequenceFile.createWriter(fs,
                conf, path, IntWritable.class, Text.class,
                SequenceFile.CompressionType.BLOCK, //块压缩
                new GzipCodec());
        for(int i = 0; i < 10; i++) {
            writer.append(new IntWritable(i), new Text("tom" + i));
        }

        for(int i = 0; i < 10; i++) {
            if(i % 2 == 0)
                writer.sync();  //每隔一个位置插入一个同步点
            writer.append(new IntWritable(i), new Text("tom" + i));
        }

        writer.close();

    }

    @Test
    public void read() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");    //本地文件系统
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("F:/bigdata/test/seq/t.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        long pos = reader.getPosition();    // 当前指针位置
        while (reader.next(key, value)) {
            String syncSeen = reader.syncSeen() ? "*" : ""; //同步点标志
            System.out.printf("[%s%s]\t%s\t%s\n", pos, syncSeen, key, value);
            pos = reader.getPosition();     //下一个记录位置
        }

        /*
        非Writable类型的序列化框架
        while (reader.next(key)) {
            String syncSeen = reader.syncSeen() ? "*" : ""; //同步点标志
            reader.getCurrentValue(value);  //获取value值
            System.out.printf("[%s%s]\t%s\t%s\n", pos, syncSeen, key, value);
            pos = reader.getPosition();     //下一个记录位置
        }
         */

        System.out.println("end pos:"+pos);
        reader.close();
    }

    /**
     * 寻找同步点
     * @throws IOException
     */
    @Test
    public void read2() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("F:/bigdata/test/seq/t.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

//        reader.seek(153);   //设置位置，153:记录起点，直接从153位置处读取
//        reader.seek(170);       //170:非记录起点，抛出异常
        reader.sync(425);       // 寻找170之后的下一个同步点，从同步点开始读取，若无同步点，指向末尾


        long pos = reader.getPosition();    // 当前指针位置
        while (reader.next(key)) {
            String syncSeen = reader.syncSeen() ? "*" : ""; //同步点标志
            reader.getCurrentValue(value);  //获取value值
            System.out.printf("[%s%s]\t%s\t%s\n", pos, syncSeen, key, value);
            pos = reader.getPosition();     //下一个记录位置
        }

        System.out.println("end pos:"+pos);
        reader.close();
    }
}
