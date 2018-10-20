package com.huliang.stormdemo.grouping.secondjoin;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 根据spout和blot构建Topology处理数据
 *
 * @author huliang
 * @date 2018/10/19 13:43
 */
public class WCApp {

    /**
     * 分组Grouping
     * 1. WordSpout -> WordSplitBolt: 2个spout分别发送4条数据(共8条)，到3个splitbolt, 采用shuffle随机分组
     * 2. WordSplitBolt -> WordCounterBolt: 按field分组，3个split发送的单词根据单词发送到3个counter，相同单词发送到同一个counter
     */
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordSource", new WordSpout(), 1).setNumTasks(1);
        builder.setBolt("wordSplit", new WordSplitBolt(), 3).shuffleGrouping("wordSource").setNumTasks(3);
        // 一次shuffle随机分组，预聚合
        builder.setBolt("wordCounter-1", new WordCounterBolt(), 3).shuffleGrouping("wordSplit").setNumTasks(3);
        // 二次field字段分组，汇总
        builder.setBolt("wordCounter-2", new WordCounterBolt(), 3).fieldsGrouping("wordCounter-1", new Fields("word")).setNumTasks(3);

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}