package com.huliang.stormdemo.grouping.global;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 根据spout和blot构建Topology处理数据
 * global grouping
 *
 * @author huliang
 * @date 2018/10/19 13:43
 */
public class WCApp {

    /**
     * all Grouping
     * WordSpout -> WordSplitBolt: 2个spout分别发送4条数据(共8条)，到3个splitbolt,
     * 采用all Grouping,每一个splitbolt都收到8条数据
     */
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordSource", new WordSpout(), 1).setNumTasks(1);
        builder.setBolt("wordSplit", new WordSplitBolt(), 3).globalGrouping("wordSource").setNumTasks(3);

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}