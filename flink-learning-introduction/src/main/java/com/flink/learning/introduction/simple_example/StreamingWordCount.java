package com.flink.learning.introduction.simple_example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/3/14 6:08 PM
 */
public class StreamingWordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("--------------senv-------------");
        System.out.println(senv);
        // 设置socket数据源
        DataStreamSource<String> source = senv.socketTextStream("127.0.0.1",8000, "\n");
        DataStream<WordWithCount> words = source.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
                for (String word : line.split(",")) {
                    collector.collect(new WordWithCount(word, 1));
                }
            }
        });
        DataStream<WordWithCount> counts = words.keyBy("word").sum("count");
        counts.print();
        // 执行任务操作
        senv.execute("Flink Streaming Word Count By Java");
    }

    public static class WordWithCount{
        public String word;
        public int count;

        public WordWithCount(){

        }

        public WordWithCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
