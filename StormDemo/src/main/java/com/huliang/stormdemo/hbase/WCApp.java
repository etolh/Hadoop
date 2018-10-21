package com.huliang.stormdemo.hbase;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * 根据spout和blot构建Topology处理数据
 * storm bolt输出数据存储到Hbase中
 *
 * @author huliang
 */
public class WCApp {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("wordSource", new WordSpout(), 1).setNumTasks(1);
        builder.setBolt("wordSplit", new WordSplitBolt(), 2).shuffleGrouping("wordSource").setNumTasks(2);
        builder.setBolt("HBaseBolt", new HBaseBolt(), 2).fieldsGrouping("wordSplit", new Fields("word")).setNumTasks(2);

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
        System.out.println("topology begins!!!");

    }
}