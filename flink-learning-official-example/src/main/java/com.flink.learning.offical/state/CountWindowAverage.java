package com.flink.learning.offical.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @description: 计算窗口平均值，Flink官方 manage keyed state官方示例
 * @author: fanyeuxiang
 * @createDate: 2020-05-27 20:55
 */
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     *
     * ValueState存储一个值的状态
     */
    private transient ValueState<Tuple2<Long, Long>> sum;


    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += value.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration configuration){
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 何时更新过期State
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //访问过期数据时的处理策略
                .build();
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average", // the state name
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                Tuple2.of(0L, 0L)
                ); // default value of the state, if nothing was set
        // 设置状态TTL策略
        descriptor.enableTimeToLive(ttlConfig);
        sum = getRuntimeContext().getState(descriptor);
    }
}
