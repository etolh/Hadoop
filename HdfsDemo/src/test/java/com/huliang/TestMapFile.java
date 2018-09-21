package com.huliang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

/**
 * @author huliang
 * @date 2018/9/21 16:12
 */
public class TestMapFile {

    @Test
    public void write() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");    //本地文件系统
        FileSystem fs = FileSystem.get(conf);
        MapFile.Writer writer = new MapFile.Writer(conf, fs, "F:/bigdata/test/map", IntWritable.class, Text.class);
//        writer.append(new IntWritable(1), new Text("tom" + 1));
//        writer.append(new IntWritable(2), new Text("tom" + 2));
        for(int i = 0; i < 100; i++) {
            writer.append(new IntWritable(i), new Text("tom" + i));
        }
        writer.close();
    }

    @Test
    public void read() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");    //本地文件系统
        FileSystem fs = FileSystem.get(conf);
        MapFile.Reader reader = new MapFile.Reader(fs,"F:/bigdata/test/map", conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

        while(reader.next(key, value)) {
            System.out.printf("%s\t%s\n", key.get(), value.toString());
        }
        reader.close();
    }
}
