package com.huliang.stormdemo.ensure;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 根据spout和blot构建Topology处理数据
 * 保证消息确定被消费
 *
 * @author huliang
 */
public class WCApp {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordSource", new WordSpout(), 1).setNumTasks(1);
        builder.setBolt("wordSplit", new WordSplitBolt(), 3).shuffleGrouping("wordSource").setNumTasks(3);

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
        System.out.println("topology begins!!!");
    }
}