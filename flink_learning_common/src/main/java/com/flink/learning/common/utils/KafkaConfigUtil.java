package com.flink.learning.common.utils;

import com.flink.learning.common.constant.PropertiesConstants;
import com.flink.learning.common.model.MetricEvent;
import com.flink.learning.common.schemas.MetricSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class KafkaConfigUtil {


    public static Properties buildKafkaProps(ParameterTool parameterTool){
        Properties properties = parameterTool.getProperties();
        properties.put("bootstrap.servers",parameterTool.get(PropertiesConstants.KAFKA_BROKERS, PropertiesConstants.DEFAULT_KAFKA_BROKERS));
        properties.put("zookeeper.connect", parameterTool.get(PropertiesConstants.KAFKA_ZOOKEEPER_CONNECT, PropertiesConstants.DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        properties.put("group.id", parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID, PropertiesConstants.DEFAULT_KAFKA_GROUP_ID));
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        return properties;
    }

    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env) throws IllegalAccessException{
        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameter.getRequired(PropertiesConstants.METRICS_TOPIC);
        Long time = parameter.getLong(PropertiesConstants.CONSUMER_FROM_TIME, 0L);
        return buildSource(env, topic, time);
    }

    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env, String topic, Long time) throws IllegalAccessException {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        FlinkKafkaConsumer010<MetricEvent> consumer = new FlinkKafkaConsumer010<MetricEvent>(topic, new MetricSchema(), props);
        //重置offset到time时刻
        if (time != 0L){
            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(props, parameterTool, time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return env.addSource(consumer);
    }

    /**
     * 构建Kafka消费者偏移量
     *
     * @param props
     * @param parameterTool
     * @param time
     * @return
     */
    private static Map<KafkaTopicPartition, Long> buildOffsetByTime(Properties props, ParameterTool parameterTool, Long time) {
        props.setProperty("group.id", "query_time_" + time);
        KafkaConsumer consumer = new KafkaConsumer(props);
        // 获取指定Topic的分区
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(parameterTool.getRequired(PropertiesConstants.METRICS_TOPIC));
        Map<TopicPartition, Long> partitionInfoLongMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionsFor) {
            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();
        offsetResult.forEach((key, value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(), key.partition()), value.offset()));
        consumer.close();
        return partitionOffset;
    }

}
