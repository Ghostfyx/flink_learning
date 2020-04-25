package com.flink.learning.datasink.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class MySink extends RichSinkFunction<String> {

    private String tx;

    //这种情况下无问题
    public MySink(String xxxx) {
        System.out.println("+++++++++++++" + xxxx);
        tx = xxxx;
    }

    //这种情况下有问题，他会将 tx 识别错误，导致下面赋值失败
//    public MySink(String tx) {
//        System.out.println("+++++++++++++" + tx);
//        tx = tx;
//    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tx = "5";
        System.out.println("========");
        super.open(parameters);
    }

    /**
     * 操作传输到sink的数据，在此仅打印数据
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(value + " " + tx);
    }

}
