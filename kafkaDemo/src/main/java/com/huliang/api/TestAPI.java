package com.huliang.api;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Java API操作Kafka：充当producer和consumer
 * @author huliang
 * @date 2018/10/17 19:33
 */
public class TestAPI {

    /**
     * 测试producer
     */
    public void testProducer() {

        Properties props = new Properties();
        // broker列表
        props.put("metadata.broker.list", "s202:9092");
        // 串行化
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 设置acks个数表示已生产完毕
        props.put("request.required.acks", "1");

        // 创建生产者配置对象
        ProducerConfig config = new ProducerConfig(props);
        // 创建生产者对象
        Producer<String, String> producer = new Producer<String, String>(config);
        // 配置消息
        KeyedMessage<String, String> msg = new KeyedMessage<String, String>("test","100" ,"hello world kafka");
        // 发送
        producer.send(msg);
        System.out.println("send over!");
    }

    /**
     * 测试consumer
     * 分组 g1
     */
    public void testConsumer() {

        Properties props = new Properties();
        // zk服务器地址
        props.put("zookeeper.connect", "s202:2181");
        // 为本机用户设置分组，(指定任意组名)，一组内只有一个consumer才能获取数据
        props.put("group.id", "g1");
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
        TestAPI test = new TestAPI();
//        test.testProducer();
        test.testConsumer();
    }
}
