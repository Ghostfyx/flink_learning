package com.flink.learning.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class Main {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 反序列化
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer010(
                "metric",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props))
                .setParallelism(1);

        dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台

        env.execute("Flink add data source");
    }

}
