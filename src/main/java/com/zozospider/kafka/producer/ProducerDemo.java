package com.zozospider.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 创建生产者
        //    配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "vm017:9092,vm06:9092,vm03:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "-1");
        //    配置 - 指定分区类
        properties.put("partitioner.class", "com.zozospider.kafka.producer.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 生产数据
        //    数据
        ProducerRecord<String, String> record1 = new ProducerRecord<>("topic1", "Value 1 From Java Client");
        //    数据 - 指定分区号
        ProducerRecord<String, String> record2 = new ProducerRecord<>("topic1", 1,null, "Value 2 From Java Client");

        //    发送 - 1. 同步
        // producer.send(record1).get();

        //    发送 - 2. 异步
        // producer.send(record1);

        //    发送 - 3. 异步回调
        producer.send(record1, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println("metadata: " + metadata);
                System.out.println("partition: " + metadata.partition());
                System.out.println("offset: " + metadata.offset());
                // metadata: topic1-0@0
                // partition: 0
                // offset: 0
            }
        });

        producer.close();

        /**
         * [zozo@vm06 kafka_2.12-2.1.0]$ bin/kafka-console-consumer.sh --bootstrap-server vm017:9092 --topic topic1 --from-beginning
         * Value 1 From Java Client
         */
    }

}
