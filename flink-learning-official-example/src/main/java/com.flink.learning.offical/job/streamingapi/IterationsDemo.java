package com.flink.learning.offical.job.streamingapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-05-18
 */
public class IterationsDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> somIntegers = env.generateSequence(0, 10);
        IterativeStream<Long> iteration = somIntegers.iterate();
        DataStream<Long> minusOne = iteration.map(value -> value-1);
        DataStream<Long> stillGreaterThanZero = minusOne.filter(value ->{ return value > 0;});
        // closeWith方法关闭iteration，方法参数部分
        iteration.closeWith(stillGreaterThanZero);
        DataStream<Long> lessThanZero = minusOne.filter(value -> {return value <= 0;});
        env.execute("IterationsDemo");
    }

}
