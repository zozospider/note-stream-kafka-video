package com.zozospider.kafka.stream;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 流处理流程:
 * Producer -> topic1 -> Processor -> topic2 -> Consumer
 */
public class StreamDemo {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "vm017:9092,vm06:9092,vm03:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "LogFilter");

        // 创建拓扑构建器
        // 不同版本 API 不一致
        // TODO
    }

}

class LogFilterProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {

        // 获取并转换 topic1 内容
        String valueStr = new String(value, StandardCharsets.UTF_8);
        valueStr = valueStr.replaceAll("#", "");

        // 输出到 topic2
        context.forward(key, valueStr.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() {

    }

}
