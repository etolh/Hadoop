package com.huliang.stormdemo.wordcount;

import com.huliang.stormdemo.util.InfoUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 生产words源的spout
 * 调用InfoUtil发送执行信息到nc服务器:7777端口
 *
 * @author huliang
 * @date 2018/10/19 13:23
 */
public class WordSpout implements IRichSpout {

    private SpoutOutputCollector collector;  // 输出收集器
    private TopologyContext context;    // 上下文
    private Random randomGeneator;      // 随机产生器
    private List<String> wordSource;    // word源

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        InfoUtil.sendToNcServer(this, "open()", 7777);

        this.context = context;
        this.collector = collector;
        this.randomGeneator = new Random();

        wordSource = new ArrayList<String>();
        wordSource.add("this is test1");
        wordSource.add("this is test2");
        wordSource.add("this is test3");
        wordSource.add("this is test4");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    // 每次调用nextTuple产生tuple数据源，源源不断产生流
    public void nextTuple() {
        InfoUtil.sendToNcServer(this, "nextTuple()", 7777);

        String word = wordSource.get(randomGeneator.nextInt(4));    // 随机产生数据
        collector.emit(new Values(word));
    }

    // 返回ack
    public void ack(Object msgId) {
        System.out.println("spout : ack = "+ msgId);
    }

    public void fail(Object msgId) {

    }

    // 声明输出tuple的字段类型
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lines"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
