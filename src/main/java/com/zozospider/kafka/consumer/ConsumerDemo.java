package com.zozospider.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        // 创建消费者
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "vm017:9092,vm06:9092,vm03:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "group1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Arrays.asList("topic1"));

        // 循环消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
        // ConsumerRecord(topic = topic1, partition = 1, leaderEpoch = 0, offset = 11, CreateTime = 1577281518040, serialized key size = -1, serialized value size = 31, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = Message 1 from console producer)
        // ConsumerRecord(topic = topic1, partition = 0, leaderEpoch = 0, offset = 6, CreateTime = 1577281531433, serialized key size = -1, serialized value size = 31, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = Message 2 from console producer)
    }

}
