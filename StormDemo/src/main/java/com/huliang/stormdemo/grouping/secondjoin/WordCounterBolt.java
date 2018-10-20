package com.huliang.stormdemo.grouping.secondjoin;

import com.huliang.stormdemo.util.InfoUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * 接收WordSplitBolt输出的<word,1>数据，进行统计,输出<word,count>元组格式
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

        // 采用分线程，执行发送数据工作，每5秒发送一次数据，
        // 并需要注意线程安全问题，由于需要从map中取出数据发送，和execute中向map插入数据存在冲突，注意同步问题
        Thread t = new Thread() {
            public void run() {
                while (true) {
                    emitData(); // 时刻发送数据
                }
            }
        };
        t.setDaemon(true); // 设为守护进程
        t.start();
    }

    // 发送tuple数据(word,count)格式，即map
    private void emitData() {
        // 对countMap加锁，使其同步
        synchronized (countMap) {
            for (Map.Entry<String,Integer> e : countMap.entrySet()) {
                // 向下一环节发送数据
                collector.emit(new Values(e.getKey(), e.getValue()));
            }
            // 发送完一次数据后，清空当前map
            countMap.clear();
        }
        // 休眠5秒
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 接收tuple数据进行统计
    public void execute(Tuple input) {

        String word = input.getString(0);

        InfoUtil.sendToLocal(this, word);

        Integer count = input.getInteger(1);
        if(!countMap.containsKey(word)) {
            countMap.put(word, count);
        }else {
            countMap.put(word, countMap.get(word) + count);
        }
    }

    // execute运行完毕后执行
    public void cleanup() {
        // 输出
        for(Map.Entry<String,Integer> entry: countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
