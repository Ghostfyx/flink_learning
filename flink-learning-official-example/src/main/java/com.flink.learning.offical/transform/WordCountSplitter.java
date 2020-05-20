package com.flink.learning.offical.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-05-15
 */
public class WordCountSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        for (String word : s.split(" ")){
            collector.collect(new Tuple2<>(word, 1));
        }
    }
}
