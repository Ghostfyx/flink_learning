package com.flink.learning.datasink.utils;

import com.alibaba.fastjson.JSON;
import com.flink.learning.datasink.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @description: 向Kafka topic中写入数据
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class KafkaUtil {

    /**
     * kafka broker列表
     */
    public static final String broker_list = "localhost:9092";
    /**
     * kafka topic 需要和 flink 程序用同一个 topic
     */
    public static final String topic = "student";

    public static void writeToKafka(){
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i < 100; i++){
            Student student = new Student(i, "student" + i, "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args)throws Exception{
        writeToKafka();
    }

}
