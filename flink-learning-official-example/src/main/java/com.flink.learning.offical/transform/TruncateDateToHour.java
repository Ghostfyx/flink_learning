package com.flink.learning.offical.transform;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-04-24
 */
public class TruncateDateToHour extends ScalarFunction {

    private static final long ONE_HOUR = 60 * 60 * 1000;

    public long eval(long timestamp) {
        return timestamp - (timestamp % ONE_HOUR);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP;
    }

}
