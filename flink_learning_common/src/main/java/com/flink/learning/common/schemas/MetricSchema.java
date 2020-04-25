package com.flink.learning.common.schemas;

import com.alibaba.fastjson.JSON;
import com.flink.learning.common.model.MetricEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @description: Metric Schema ，支持序列化和反序列化
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class MetricSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {


    @Override
    public MetricEvent deserialize(byte[] message) throws IOException {
        return JSON.parseObject(new String(message), MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(MetricEvent element) {
        return JSON.toJSONBytes(element);
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
