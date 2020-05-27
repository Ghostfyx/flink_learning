package com.flink.learning.offical.job;

import com.flink.learning.offical.state.CountWindowAverage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-05-27 21:06
 */
public class ManageKeyedStateJob {

    public static void main(String[] args){
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0) // 根据Tuple2的第一个元素聚合，获取keyedState
        .flatMap(new CountWindowAverage()).print();
    }

}
