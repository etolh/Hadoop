package com.huliang.stormdemo.callLog;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 根据定义的Spout和blot构建Topology,确定输出序列
 * @author huliang
 * @date 2018/10/18 20:34
 */
public class App {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 设置spout:定义spout对象及其id
        topologyBuilder.setSpout("spout", new CallLogSpout());
        // 设置blot:定义blot对象和id，并确定其数据来源和分发方式
        // 数据来源于id=spout的对象，以shuffle随机分发的方式获取数据
        topologyBuilder.setBolt("creator",new CallLogCreationBolt()).shuffleGrouping("spout");
        // 接收来自id=creator的blot对象的输出数据，creatorbolt输出到counter是根据字段call的值进行分发的
        topologyBuilder.setBolt("counter", new CallLogCounterBolt()).fieldsGrouping("creator", new Fields("call"));

        Config conf = new Config();
        conf.setDebug(true);

        // 本地模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("CallLogCounter", conf, topologyBuilder.createTopology());
//        Thread.sleep(10000); // 休息10s,用于清理数据
        System.out.println("Topology begin!!!");

        //停止集群，否则storm一直执行处理流数据
//        cluster.shutdown();

        // 集群运行
//        StormSubmitter.submitTopology("CallLogCounter",conf, topologyBuilder.createTopology());
    }
}
