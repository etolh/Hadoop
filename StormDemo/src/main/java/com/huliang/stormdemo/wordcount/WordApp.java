package com.huliang.stormdemo.wordcount;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 根据spout和blot构建Topology处理数据
 *
 * @author huliang
 * @date 2018/10/19 13:43
 */
public class WordApp {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        // 3个spout task交给3个executor执行
        builder.setSpout("wordSource", new WordSpout(), 3).setNumTasks(3);
        // 6个bolt task交给3个executor执行
        builder.setBolt("wordSplit", new WordSplitBolt(), 3).shuffleGrouping("wordSource").setNumTasks(6);
        // 9个bolt task交给3个executor执行
        // 接收来自WordSplitBolt对象emit的数据，根据word字段分组
        builder.setBolt("wordCounter", new WordCounterBolt(), 3).fieldsGrouping("wordSplit", new Fields("word")).setNumTasks(9);

        Config conf = new Config();
        // 一共启动3个worker，默认每个结点启动一个worker
        conf.setNumWorkers(3);
        conf.setDebug(true);

        /*
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
        */
        StormSubmitter.submitTopology("WordCount", conf, builder.createTopology());
    }
}