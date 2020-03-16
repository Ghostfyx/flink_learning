package com.flink.learning.datasource.util;

import com.alibaba.fastjson.JSON;
import com.flink.learning.datasource.model.MetricEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-16
 */
public class KafkaUtil {

    // 虚拟机IP
    public static final String broker_list = "master:9092";
    public static final String topic = "metric";

    public static void writeToKafka() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", broker_list);
        //key 序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        MetricEvent metric = new MetricEvent();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<String, String>();
        Map<String, Object> fields = new HashMap<String, Object>();

        tags.put("cluster", "Ghost");
        tags.put("host_ip", "127.0.0.1");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);
        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record);
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
