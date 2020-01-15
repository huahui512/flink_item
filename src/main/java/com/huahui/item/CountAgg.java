package com.huahui.item;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * @author wangzhihua
 * @date 2018-12-25 14:27
 */
public class CountAgg implements AggregateFunction<Tuple5<Long, Long, Long, String, Long>,Long,Long>{
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple5<Long, Long, Long, String, Long> tuple5,Long ACC) {
        return ACC++;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long  acc2) {
        return acc1+acc2;
    }
}
