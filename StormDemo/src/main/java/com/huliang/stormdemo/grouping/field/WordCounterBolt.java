package com.huliang.stormdemo.grouping.field;

import com.huliang.stormdemo.util.InfoUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * 接收WordSplitBolt输出的<word,1>数据，进行统计,输出<word,count>元组格式
 * 调用InfoUtil发送执行信息到nc服务器:9999端口
 * @author huliang
 * @date 2018/10/19 13:36
 */
public class WordCounterBolt implements IRichBolt {

    private OutputCollector collector;
    private TopologyContext context;
    private Map<String,Integer> countMap;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.countMap = new HashMap<String, Integer>();
    }

    // 接收tuple数据进行统计
    public void execute(Tuple input) {

        String word = input.getString(0);

        InfoUtil.sendToLocal(this, word); // 2 - field测试

        Integer count = input.getInteger(1);
        if(!countMap.containsKey(word)) {
            countMap.put(word, count);
        }else {
            countMap.put(word, countMap.get(word) + count);
        }
        // 最终bolt返回
        collector.ack(input);
    }

    // execute运行完毕后执行
    public void cleanup() {
        // 输出
        for(Map.Entry<String,Integer> entry: countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordcount"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
