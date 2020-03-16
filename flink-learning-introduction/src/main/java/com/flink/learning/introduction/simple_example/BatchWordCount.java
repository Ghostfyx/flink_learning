package com.flink.learning.introduction.simple_example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @Description: Flink入门程序 批处理wordcount
 * @Author: FanYueXiang
 * @Date: 2020/3/14 5:29 PM
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 加载或创建源数据
        DataSet<String> text = env.readTextFile("D:\\experiment\\flink_learning\\flink-learning-introduction\\src\\main\\java\\learning\\simple_example\\wordcount.txt");
        // 转化处理数据
        DataSet<Tuple2<String, Integer>> ds = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        // 输出数据到目的端
        ds.print();
        // 执行任务操作
        // 由于是Batch操作，当DataSet调用print方法时，源码内部已经调用Excute方法，所以此处不再调用，如果调用会出现错误
        //env.execute("Flink Batch Word Count By Java");

    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word:line.split(",")) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
