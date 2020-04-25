package com.flink.learning.offical.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.Objects;

/**
 * @description: 欺诈交易风险报警
 * @author: fanyeuxiang
 * @createDate: 2020-04-22
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Alert implements Serializable {

    private static final long serialVersionUID = 1516578142915942854L;

    private long id;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Alert alert = (Alert) o;
        return id == alert.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

}
