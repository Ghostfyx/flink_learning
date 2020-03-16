package com.flink.learning.datasource.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    public Long id;

    public String name;

    public String password;

    public Integer age;
}
