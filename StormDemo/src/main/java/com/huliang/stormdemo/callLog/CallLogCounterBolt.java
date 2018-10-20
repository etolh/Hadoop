package com.huliang.stormdemo.callLog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * 统计通话日志Blot: 接收CreationBolt发送的tuple数据，统计每条通话记录(from-to)的个数
 * @author huliang
 * @date 2018/10/18 20:25
 */
public class CallLogCounterBolt implements IRichBolt {

    private Map<String, Integer> counterMap;
    private OutputCollector collector;      // 输出收集器

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    /**
     * 接收tuple进行统计处理
     */
    public void execute(Tuple input) {

        String call = input.getString(0);
        Integer duration = input.getInteger(1);

        if(!counterMap.containsKey(call)) {
            counterMap.put(call, new Integer(1));
        }else{
            int old = counterMap.get(call);
            counterMap.put(call, new Integer(old+1));
        }
        // 最后一个bolt，返回ack数据
        collector.ack(input);
    }

    /**
     * 最后Bolt，用于数据清理
     */
    public void cleanup() {
        // 打印
        for(Map.Entry<String,Integer> entry : counterMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    /**
     * 定义输出的字段名称
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
