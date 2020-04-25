package com.flink.learning.datasource;

import com.flink.learning.datasource.sources.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class Main2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.addSource(new SourceFromMySQL()).print();
        senv.execute("Flink add data sourc");
    }

}
