package com.flink.learning.offical.transform;

import com.flink.learning.offical.domain.Alert;
import com.flink.learning.offical.domain.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description: 使用计数器和Flink状态类型来检测欺诈交易
 * @author: fanyeuxiang
 * @createDate: 2020-04-22
 */
public class FraudDetectorV2 extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = -2468030140074900904L;
    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    /**
     * Flink基本状态类型ValueState
     */
    private transient ValueState<Boolean> flagState;
    /**
     * Flink计数器
     */
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();
        // Check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                //Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            // Clean up our state
            cleanUp(context);
        }
        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true);
            /**
             * 关键代码：
             * 1. 获取当前系统时间与状态过期时间；
             * 2. 向KeyedProcessFunction上下文中注册时间定时服务；
             */
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }
    }

    /**
     * 计时器出发时的回调方法，欺诈交易检测中为了删除一分钟前的状态
     *
     * @param timestamp
     * @param ctx
     * @param out
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // remove flag after 1 minute
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);
        // clean up all state
        timerState.clear();
        flagState.clear();
    }
}
