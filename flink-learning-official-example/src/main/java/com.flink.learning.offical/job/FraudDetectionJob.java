package com.flink.learning.offical.job;

import com.flink.learning.offical.domain.Alert;
import com.flink.learning.offical.domain.Transaction;
import com.flink.learning.offical.sink.AlertSinkV0;
import com.flink.learning.offical.source.TransactionSource;
import com.flink.learning.offical.transform.FraudDetectorV0;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @description: 金融欺诈实时检测示例代码
 * @author: fanyeuxiang
 * @createDate: 2020-04-22
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Transaction> transactionDataStream = env.addSource(new TransactionSource()).name("transactionSource");

        DataStream<Alert> alerts = transactionDataStream.keyBy(Transaction::getAccountId).process(new FraudDetectorV0()).name("fraudDetectorV0");

        alerts.addSink(new AlertSinkV0()).name("alertSinkV0");

        env.execute("Fraud Detection");
    }

}
