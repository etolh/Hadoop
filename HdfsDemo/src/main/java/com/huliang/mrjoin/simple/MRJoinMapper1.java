package com.huliang.mrjoin.simple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 *  大文件+小文件Mapper:
 *  小文件直接读取到内存，大文件Mapper逐条读取
 *
 *  如例：customers.txt直接读取，保存到HashMap
 *  orders.txt逐条读取，根据Oder找到对应的Customer进行组合，进行输出。
 *
 *  输入:<1,(oid,..)> 输出：<Text(cid, cname, oid, price),NullWritable>
 * @author huliang
 * @date 2018/10/2 16:27
 */
public class MRJoinMapper1 extends Mapper<LongWritable, Text, Text, NullWritable> {

    HashMap<String,String> customerMap = new HashMap<String, String>();

    /**
     * 读取customer.txt，创建HashMap<cid,cinfo>
     */
    protected void setup(Context context) throws IOException, InterruptedException {

        BufferedReader bfr = new BufferedReader( new FileReader("F:\\bigdata\\test\\mrjoin\\customers.txt"));
        String line = null;
        // 逐行读取
        while((line = bfr.readLine()) != null) {
            String cid = line.substring(0, line.indexOf(','));
            String cinfo = line.substring(line.indexOf(',')+1);
            customerMap.put(cid, cinfo);
        }
        bfr.close();
    }

    /**
     * 读取orders.txt行，根据cid查找与hashmap匹配的用户进行组合
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String orderLine  = value.toString();
        String oid = orderLine.substring(0, orderLine.indexOf(','));
        String cid = orderLine.substring(orderLine.lastIndexOf(',')+1);
        String oinfo = orderLine.substring(orderLine.indexOf(',')+1, orderLine.lastIndexOf(','));

        String cinfo = customerMap.get(cid);
        if(cinfo != null) {
            // 不空，内连接
            String info = cid + ":" + cinfo + "/" + oid +":"+oinfo;
            context.write(new Text(info), NullWritable.get());
        }
    }
}
