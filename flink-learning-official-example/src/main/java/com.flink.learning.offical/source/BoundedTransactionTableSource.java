package com.flink.learning.offical.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-04-24
 */
@PublicEvolving
@SuppressWarnings({"deprecation", "unused"})
public class BoundedTransactionTableSource extends InputFormatTableSource<Row> {
    @Override
    public InputFormat<Row, ?> getInputFormat() {
        return new TransactionRowInputFormat();
    }

    @Override
    public DataType getProducedDataType() {
        return getTableSchema().toRowDataType();
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .field("accountId", Types.LONG)
                .field("timestamp", Types.SQL_TIMESTAMP)
                .field("amount", Types.DOUBLE)
                .build();
    }

}
