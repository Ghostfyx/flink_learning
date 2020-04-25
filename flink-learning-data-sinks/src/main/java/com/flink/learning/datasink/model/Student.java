package com.flink.learning.datasink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    public int id;

    public String name;

    public String password;

    public int age;
}
