package com.huliang.stormdemo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.testng.annotations.ITestOrConfiguration;

import java.io.IOException;
import java.util.Map;

/**
 * HbaseBolt接收splitBolt数据，存储到Hbase数据库中
 * @author huliang
 * @date 2018/10/21 21:33
 */
public class HBaseBolt implements IRichBolt {

    private Table counter;

    /**
     * 创建Hbase Table的连接
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            TableName tableName = TableName.valueOf("ns1:counter");
            counter = conn.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *  接收splitbolt数据，存入Hbase数据库
     *  Table数据库格式： ns1:stormCounter 列族：f1 列：count, rowKey设为word
     */
    public void execute(Tuple input) {

        String word = input.getString(0);
        Integer count = input.getInteger(1);

        try {
            counter.incrementColumnValue(Bytes.toBytes(word), Bytes.toBytes("f1"), Bytes.toBytes("count"),count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
