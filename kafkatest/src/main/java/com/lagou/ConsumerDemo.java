package com.lagou;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        // 消息消费者对象
        //  属性对象
        Properties properties = new Properties();
        //      指定集群节点
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.200.20:9092,192.168.200.20:9093,192.168.200.20:9094");
        //      接收消息, 网络传输, 需要反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //      取消自动提交(offset)
        //properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //      指定分组的名称(只有消费者指定组名)(避免重复消费)
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"lagou_group1");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // 订阅消息 (指定主题)
        consumer.subscribe(Collections.singletonList("lagou"));

        // 接收消息
        while(true){
            // 拉取数据
            ConsumerRecords<String, String> records = consumer.poll(500);
            for(ConsumerRecord<String,String> record: records){
                System.out.println("主题: " + record.topic() +
                        ", 偏移量: " + record.offset() +
                        ", msg: " + record.value());
                // 手动提交
            }
        }
        // consumer.close();
    }
}
