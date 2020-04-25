package com.flink.learning.common.utils;

import com.flink.learning.common.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class ExecutionEnvUtil {

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool.fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool){
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        senv.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        senv.getConfig().disableSysoutLogging();
        /**
         * 设置作业遇到故障重启策略:
         * 1. 固定间隔 (Fixed delay)
         * 2. 失败率 (Failure rate)
         * 3. 无重启 (No restart): 如果没有启用 checkpointing机制，则使用无重启 (no restart) 策略。
         * 本例使用的是固定间隔：设置重试次数和两次重试之间的时间间隔
         **/
        senv.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));
        senv.getConfig().setGlobalJobParameters(parameterTool);
        // 设置Flink流式处理时间戳和水印
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return senv;
    }

}
