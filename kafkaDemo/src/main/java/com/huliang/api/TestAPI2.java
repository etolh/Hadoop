package com.huliang.api;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Java API操作Kafka：充当producer和consumer
 * @author huliang
 * @date 2018/10/17 19:33
 */
public class TestAPI2 {


    /**
     * 测试分组情况 g2
     */
    public void testConsumer() {

        Properties props = new Properties();
        // zk服务器地址
        props.put("zookeeper.connect", "s202:2181");
        // 为本机用户设置分组，(指定任意组名)，一组内只有一个consumer才能获取数据
        props.put("group.id", "g2");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        // 创建消费者配置信息
        ConsumerConfig config = new ConsumerConfig(props);
        // 接收消息
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("test",new Integer(1)); // 定义topics
        // 接收消息
        Map<String, List<KafkaStream<byte[], byte[]>>> msgs =  Consumer.createJavaConsumerConnector(config).createMessageStreams(map);
        List<KafkaStream<byte[], byte[]>> msgList = msgs.get("test");   // 获取指定topic的消息

        for( KafkaStream<byte[], byte[]> stream: msgList) {
            ConsumerIterator<byte[],byte[]> it = stream.iterator();
            while (it.hasNext()) {
                byte[] message = it.next().message();
                System.out.println(new String(message));
            }
        }
    }

    public static void main(String[] args) {
        TestAPI2 test = new TestAPI2();
//        test.testProducer();
        test.testConsumer();
    }
}
