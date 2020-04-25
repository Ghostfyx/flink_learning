package com.flink.learning.offical.job;

import com.flink.learning.offical.source.BoundedTransactionTableSource;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-04-23 15:45
 */
public class SpendReport {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        tEnv.registerTableSource("transactions", new BoundedTransactionTableSource());
        TableSink sink = new CsvTableSink("out.csv", "|");
        tEnv.registerTableSink("spend_report", sink);

        tEnv
                .scan("transactions")
                .window(Tumble.over("1.hour").on("timestamp").as("w"))
                .groupBy("accountId, w")
                .select("accountId, w.start as timestamp, amount.sum")
                .insertInto("spend_report");

        env.execute("Spend Report");
    }

}
