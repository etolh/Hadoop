package com.huliang.mrjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 接收txt文本每行信息，形成CusOrdKey输出
 * @author huliang
 * @date 2018/10/2 17:06
 */
public class MRJoinMapper2 extends Mapper<LongWritable, Text, CusOrdKey, NullWritable> {

    // 根据文件名判断读取类型customer|order,
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        FileSplit split = (FileSplit) context.getInputSplit();
        String path = split.getPath().toString();

        CusOrdKey cusOrdKey = new CusOrdKey();
        if(path.contains("customers")) {
            cusOrdKey.setType(0); // 用户
            String cid = line.substring(0, line.indexOf(','));
            String cinfo = line.substring(line.lastIndexOf(',')+1);
            cusOrdKey.setCid(Integer.parseInt(cid));
            cusOrdKey.setCinfo(cinfo);
        }else {
            cusOrdKey.setType(1);  // 订单
            int opos = line.indexOf(',');
            int cpos = line.lastIndexOf(',');
            String oid = line.substring(0, opos);
            String cid = line.substring(cpos+1);
            String oinfo = line.substring(opos+1, cpos);
            cusOrdKey.setOid(Integer.parseInt(oid));
            cusOrdKey.setCid(Integer.parseInt(cid));
            cusOrdKey.setOinfo(oinfo);
        }
        context.write(cusOrdKey, NullWritable.get());
    }
}
