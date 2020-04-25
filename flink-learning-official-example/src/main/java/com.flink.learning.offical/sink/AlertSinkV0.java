package com.flink.learning.offical.sink;

import com.flink.learning.offical.domain.Alert;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description: 欺诈报警信息对外
 * @author: fanyeuxiang
 * @createDate: 2020-04-22
 */
public class AlertSinkV0 implements SinkFunction<Alert> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertSinkV0.class);

    @Override
    public void invoke(Alert value, Context context) {
        // 第一版只打印日志到控制台
        LOG.info(value.toString());
    }
}
