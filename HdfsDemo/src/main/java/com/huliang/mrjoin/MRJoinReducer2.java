package com.huliang.mrjoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 接收一组同cid 的<CusOrdKey, NullWritable>，首位是用户，后面是其对应的订单，进行组合输出。
 *
 * @author huliang
 * @date 2018/10/2 18:08
 */
public class MRJoinReducer2 extends Reducer<CusOrdKey, NullWritable, Text, NullWritable> {

    @Override
    // 获取一组首位CusOrdKey为用户，对后面订单CusOrdKey获取信息进行组合
    protected void reduce(CusOrdKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<NullWritable> it = values.iterator();
        it.next();
        int cid = key.getCid();  //用户
        String cinfo = key.getCinfo();

        // key不断迭代
        while (it.hasNext()) {
            it.next();
            int oid = key.getOid();
            String oinfo = key.getOinfo();
            String text = cid + ":" + cinfo + "/" + oid +":"+oinfo;
            context.write(new Text(text), NullWritable.get());
        }
    }
}
