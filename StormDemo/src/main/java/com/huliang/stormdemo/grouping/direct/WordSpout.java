package com.huliang.stormdemo.grouping.direct;

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
    private List<String> wordSource;    // word源
    private int idx;                    // 统计发送tuple个数

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.context = context;
        this.collector = collector;

        wordSource = new ArrayList<String>();
        wordSource.add("this is test1");
        wordSource.add("hello tom");
        wordSource.add("tomas");
        wordSource.add("guns");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    // 每次调用nextTuple产生tuple数据源，源源不断产生流
    public void nextTuple() {

        // 获取要发送数据的下一个bolt的taskId
        int taskId = 0;
        Map<Integer, String> map = context.getTaskToComponent(); // task id to component id.
        for(Map.Entry<Integer, String> e: map.entrySet()) {
            if(e.getValue().equals("wordSplit")) {   // App设置的Bolt Id
                taskId = e.getKey();
                break;  // 获取下一层第一个Bolt对象Id
            }
        }

        if(idx < 4) {
            String line = wordSource.get(idx);      // 每条数据发送一次
//            InfoUtil.sendToLocal(this, line);  // 发送到本地, 1-shuffle测试
//            collector.emit(new Values(line));
            collector.emitDirect(taskId, new Values(line));
            idx++;
        }
    }

    // 返回ack
    public void ack(Object msgId) {
        System.out.println("spout : ack = "+ msgId);
    }

    public void fail(Object msgId) {

    }

    // 声明输出tuple的字段类型
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
