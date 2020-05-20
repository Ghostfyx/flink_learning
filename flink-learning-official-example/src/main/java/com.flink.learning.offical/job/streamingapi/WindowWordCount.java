package com.flink.learning.offical.job.streamingapi;

import com.flink.learning.offical.transform.WordCountSplitter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @description: Flink DataStream API Programming Guide
 * @author: fanyeuxiang
 * @createDate: 2020-05-15
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = senv.socketTextStream("master", 9999)
                .flatMap(new WordCountSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(2))
                .sum(1);
        dataStream.print();
        senv.execute("Window WordCount");
    }

}
