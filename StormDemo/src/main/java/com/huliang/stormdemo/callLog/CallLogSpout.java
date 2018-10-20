package com.huliang.stormdemo.callLog;

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
 * 电话记录数据生产源
 * @author huliang
 * @date 2018/10/18 17:11
 */
public class CallLogSpout implements IRichSpout {

    // spout输出收集器
    private SpoutOutputCollector collector;
    // 是否完成
    private boolean completed = false;
    // 上下文
    private TopologyContext context;
    // 随机发送器
    private Random randomGenerator = new Random();
    // 索引，统计发送数据
    private Integer idx = 0;

    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    /**
     * 输出下一个元组tuple
     */
    public void nextTuple() {

        if(this.idx <= 1000) {

            List<String> mobileNumbers = new ArrayList<String>(); // 电话簿
            mobileNumbers.add("1234123401");
            mobileNumbers.add("1234123402");
            mobileNumbers.add("1234123403");
            mobileNumbers.add("1234123404");

            Integer localIdx = 0;
            while (localIdx++ < 100 && this.idx++ < 1000) {
                // 呼叫者
                String caller = mobileNumbers.get(randomGenerator.nextInt(4));
                // 被叫者
                String callee = mobileNumbers.get(randomGenerator.nextInt(4));
                while (caller == callee) {
                    //重新取出被叫
                    callee = mobileNumbers.get(randomGenerator.nextInt(4));
                }
                //模拟通话时长
                Integer duration = randomGenerator.nextInt(60);

                // 输出元组
                this.collector.emit(new Values(caller, callee, duration));
            }
        }
    }

    // ack确认
    public void ack(Object misId) {
        System.out.println("spout : ack = "+ misId);
    }

    public void fail(Object o) {

    }

    /**
     * 定义输出的字段名称
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from","to","duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
