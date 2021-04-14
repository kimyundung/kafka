package com.lagou;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // 创建消息生产者对象 (连接谁?消息序列化?=>properties)
        //  kafka集群等相关配置, 可以从properties文件中加载也可以从一个properties对象中加载
        //  KafkaProducer 按照固定的key取出对应的value
        Properties properties = new Properties();
        //      指定集群节点
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.200.20:9092,192.168.200.20:9093,192.168.200.20:9094");
        //      发送消息, 网络传输, 需要对key和value指定对应的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // 发送100条消息
        String topic = "lagou";
        for(int i=1; i<=100; i++){
            String msg = "hello, " + i;
            //  消息对象: 主题(如果不存在, kafka自动创建一个分区一个副本的主题) + 消息
            ProducerRecord<String,String> record = new ProducerRecord<>(topic,msg);
            //  发送
            producer.send(record);
            System.out.println("消息发送成功, msg: "+topic+" ->"+ msg);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // 关闭消息生产者对象
        producer.close();
    }
}
