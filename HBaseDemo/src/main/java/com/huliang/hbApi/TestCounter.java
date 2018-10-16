package com.huliang.hbApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 计数器
 * @author huliang
 * @date 2018/10/13 21:14
 */
public class TestCounter {

    public void testCounter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:counter");
        Table counter = conn.getTable(tableName);

        // 创建Increment对象，进行增加操作
        Increment incr = new Increment(Bytes.toBytes("1013"));
        incr.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1L);
        incr.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 10L);
        incr.addColumn(Bytes.toBytes("monthly"), Bytes.toBytes("hits"), 100L);
        counter.increment(incr);
    }

    public static void main(String[] args) throws Exception {
        TestCounter test = new TestCounter();
        test.testCounter();
    }
}