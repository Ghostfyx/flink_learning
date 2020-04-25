package com.flink.learning.datasink;

import com.alibaba.fastjson.JSON;
import com.flink.learning.common.constant.PropertiesConstants;
import com.flink.learning.common.utils.ExecutionEnvUtil;
import com.flink.learning.common.utils.KafkaConfigUtil;
import com.flink.learning.datasink.model.Student;
import com.flink.learning.datasink.sinks.MySqlSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @description: kafka Source ----> SinkToMysqlMain数据处理逻辑 -------> MySQLSink 写入到MySQL
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class SinkToMysqlMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer010<>(
                parameterTool.get(PropertiesConstants.METRICS_TOPIC),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                properties)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class));
        student.addSink(new MySqlSink());
        env.execute("Flink data sink");
    }

}
