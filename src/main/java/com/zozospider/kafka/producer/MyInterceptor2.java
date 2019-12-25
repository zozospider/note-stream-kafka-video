package com.zozospider.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyInterceptor2 implements ProducerInterceptor<String, String> {

    private int success;
    private int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 此处需要返回原始数据
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("数据发送成功的数量: " + success);
        System.out.println("数据发送失败的数量: " + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

}
