package com.hyj.flink.udf;

import com.hyj.flink.domain.AccessOrigin;

import org.apache.flink.api.common.functions.AggregateFunction;

public class TopNAggregateFunction implements AggregateFunction<AccessOrigin, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AccessOrigin value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
