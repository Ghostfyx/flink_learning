package com.flink.learning.offical.transform;

import com.flink.learning.offical.domain.Alert;
import com.flink.learning.offical.domain.Transaction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-04-22 14:38
 */
public class FraudDetectorV0 extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());
        collector.collect(alert);
    }
}
