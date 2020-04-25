package com.flink.learning.offical.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-04-22
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Transaction implements Serializable {

    private static final long serialVersionUID = 3166942647793966822L;

    private long accountId;

    private long timestamp;

    private double amount;
}
