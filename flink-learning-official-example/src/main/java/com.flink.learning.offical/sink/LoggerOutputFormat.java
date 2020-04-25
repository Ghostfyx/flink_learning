package com.flink.learning.offical.sink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-04-24
 */
public class LoggerOutputFormat implements OutputFormat<String> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggerOutputFormat.class);
    private static final long serialVersionUID = -7686731215936427813L;

    @Override
    public void configure(Configuration parameters) { }

    @Override
    public void open(int taskNumber, int numTasks) { }

    @Override
    public void writeRecord(String record) {
        LOG.info(record);
    }

    @Override
    public void close() { }
}
