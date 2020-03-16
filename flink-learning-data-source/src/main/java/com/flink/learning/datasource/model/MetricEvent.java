package com.flink.learning.datasource.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MetricEvent {

    private String name;

    public long timestamp;

    public Map<String, Object> fields;

    public Map<String, String> tags;

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", fields=" + fields +
                ", tags=" + tags +
                '}';
    }

}
