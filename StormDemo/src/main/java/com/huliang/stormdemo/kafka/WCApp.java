package com.huliang.stormdemo.kafka;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

/**
 * 根据spout和blot构建Topology处理数据
 * Kafka作为数据源，storm作为consumer
 *
 * @author huliang
 */
public class WCApp {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //zk连接串
        String zkConnString = "s202:2181" ;
        BrokerHosts hosts = new ZkHosts(zkConnString);
        //Spout配置
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "test2", "/test2", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("kafkaSpout", kafkaSpout).setNumTasks(2);
        builder.setBolt("splitBolt", new WordSplitBolt(),2).shuffleGrouping("kafkaSpout").setNumTasks(2);

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
        System.out.println("topology begins!!!");
    }
}