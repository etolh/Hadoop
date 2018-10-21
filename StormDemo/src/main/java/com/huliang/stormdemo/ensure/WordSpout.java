package com.huliang.stormdemo.ensure;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * 生产words源的spout
 *
 *  回调ack()或fail()执行应对反应
 *  采取两个Map存储msgId与消息的映射、失败消息的msgId与重来次数的映射
 * @author huliang
 * @date 2018/10/19 13:23
 */
public class WordSpout implements IRichSpout {

    private SpoutOutputCollector collector;  // 输出收集器
    private TopologyContext context;    // 上下文
    private Random randomGeneator;      // 随机产生器
    private List<String> wordSource;    // word源
    private int idx;                    // 统计发送tuple个数

    // 消息map : 存放msgId（时间戳）与tuple数据的映射
    private Map<Long,String> messageMap = new HashMap<Long, String>();
    // 重来map : 存放msgId与重来次数的映射
    private Map<Long, Integer> failMsgMap = new HashMap<Long, Integer>();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.context = context;
        this.collector = collector;
        this.randomGeneator = new Random();

        wordSource = new ArrayList<String>();
        wordSource.add("this is test1");
        wordSource.add("hello tom2");
        wordSource.add("tomas3");
        wordSource.add("guns4");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    // 每次调用nextTuple产生tuple数据源，源源不断产生流
    public void nextTuple() {
        if(idx < 4) {
            String line = wordSource.get(idx);
            Long ts = System.currentTimeMillis();
            collector.emit(new Values(line), ts);   // 时间戳设置msgId,用于ack或fail
            System.out.println(this + "nextTuple() : " + line + ": " + ts);
            messageMap.put(ts, line);  // 增加映射
            idx++;
        }
    }

    // 消费成功回调ack
    public void ack(Object msgId) {
        System.out.println(this + "spout : ack = "+ msgId);
        // 成功，移出重发map
        Long ts = (Long)msgId;
        failMsgMap.remove(ts);
        messageMap.remove(ts);
    }

    // 消费失败回调fail,当消费失败时，获取失败msgId的重来次数，
    // 当大于3次时，移除重来map
    public void fail(Object msgId) {

        System.out.println(this + "spout : fail = "+ msgId);
        // 获取失败message id
        Long ts = (Long)msgId;
        Integer count = failMsgMap.get(ts);
        count = (count == null ? 0 : count);

        if(count > 3) {
            // 超过3次不再重发
            failMsgMap.remove(ts);
            messageMap.remove(ts);   // 移除防止数据过多
        }else {
            // 重发
            collector.emit(new Values(messageMap.get(ts)),msgId);
            count++;    // 重发次数+1
            failMsgMap.put(ts,count);
        }
    }

    // 声明输出tuple的字段类型
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
