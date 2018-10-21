package com.huliang.stormdemo.ensure;

import com.huliang.stormdemo.util.InfoUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * 接收WordSpout输出的line元组，进行split
 * 调用InfoUtil发送执行信息到nc服务器:8888端口
 * @author huliang
 * @date 2018/10/19 13:32
 */
public class WordSplitBolt implements IRichBolt {

    private OutputCollector collector;
    private TopologyContext context;

    // 预备阶段
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
    }

    // 接收lines信息进行split, 发送<word,1>格式tuple到下一个bolt
    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\s+"); // split
        for (String word : words) {
            collector.emit(new Values(word, new Integer(1)));
        }

        // ack或fail
        if(new Random().nextBoolean()) {
            collector.ack(input);
            System.out.println(this + " : ack() : " + line + " : "+ input.getMessageId().toString());
        }else {
            collector.fail(input);
            System.out.println(this + " : fail() : " + line + " : "+ input.getMessageId().toString());
        }
    }

    public void cleanup() {

    }

    // 声明输出tuple字段名称
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
