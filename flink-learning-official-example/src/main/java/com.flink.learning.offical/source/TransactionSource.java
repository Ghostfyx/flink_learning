package com.flink.learning.offical.source;

import com.flink.learning.offical.domain.Transaction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import java.io.Serializable;
import java.util.Iterator;

/**
 * @description: 金融交易数据来源
 * @author: fanyeuxiang
 * @createDate: 2020-04-22
 */
public class TransactionSource extends FromIteratorFunction<Transaction> {

    private static final long serialVersionUID = -871646770404812286L;

    public TransactionSource() {
        super(new RateLimitedIterator<>(TransactionIterator.unbounded()));
    }

    private static class RateLimitedIterator<T> implements Iterator<T>, Serializable {

        private static final long serialVersionUID = 1L;

        private final Iterator<T> inner;

        private RateLimitedIterator(Iterator<T> inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public T next() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return inner.next();
        }
    }
}
