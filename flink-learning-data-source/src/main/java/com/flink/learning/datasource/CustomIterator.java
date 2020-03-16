package com.flink.learning.datasource;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @description: 自定义生成迭代器
 * @author: fanyeuxiang
 * @createDate: 2020-03-16
 */
public class CustomIterator implements Iterator<Integer>, Serializable {

    private Integer i = 0;

    @Override
    public boolean hasNext() {
        return i < 100;
    }

    @Override
    public Integer next() {
        i++;
        return i;
    }
}
