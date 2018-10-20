package com.huliang.stormdemo.callLog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 通话记录创造类Blot: 接收Spout发送的tuple数据，创建新格式的tuple输出到Counter
 * @author huliang
 * @date 2018/10/18 20:18
 */
public class CallLogCreationBolt implements IRichBolt {

    // 输出收集器
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     *  接收spout输出元组进行处理，创建通话记录
     */
    public void execute(Tuple input) {
        String from = input.getString(0);
        String to = input.getString(1);
        Integer duration = input.getInteger(2);
        // 产生新的tuple: (from-to,duration)
        collector.emit(new Values(from+"-"+to, duration));
    }

    public void cleanup() {

    }

    /**
     * 声明输出tuple的字段名称
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call", "duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
